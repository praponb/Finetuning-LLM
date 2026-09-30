from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_site_id_by_read_at_running = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_plus_kwh = Window.partitionBy("site_id").orderBy("voltage_plus_kwh", "id")
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_reading_quality_site_id_by_r = Window.partitionBy("reading_quality", "site_id").orderBy(F.col("read_at_cume_dist").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_quality = Window.partitionBy("reading_quality")
w_reading_quality_site_id_by_r_2 = Window.partitionBy("reading_quality", "site_id").orderBy(F.col("read_at_cume_dist").desc(), "id", "reading_id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality") != "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

windowed = (
    cleaned_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality_prefix",
        F.max("kwh").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_max_kwh"),
        F.round(F.cume_dist().over(w_site_id_by_read_at), 3).alias("read_at_cume_dist"),
        F.last("voltage_plus_kwh", ignorenulls=True).over(w_site_id_by_read_at_running).alias("last_known_voltage_plus_kwh"),
        F.round(F.cume_dist().over(w_site_id_by_voltage_plus_kwh), 3).alias("voltage_plus_kwh_cume_dist"),
        F.lag("voltage_plus_kwh").over(w_all_by_read_at).alias("prev_voltage_plus_kwh"),
    )
)

with_children = (
    windowed.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality_prefix",
        "s.site_id_read_at_to_date_max_kwh",
        "s.read_at_cume_dist",
        "s.last_known_voltage_plus_kwh",
        "s.voltage_plus_kwh_cume_dist",
        "s.prev_voltage_plus_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

sequenced = (
    with_children
    .select(
        "id",
        "kwh",
        "voltage",
        "quality_prefix",
        "read_at_cume_dist",
        "last_known_voltage_plus_kwh",
        "prev_voltage_plus_kwh",
        "reading_id",
        "reading_read_at",
        "reading_kwh",
        "reading_voltage",
        F.nth_value("kwh", 2).over(w_reading_quality_site_id_by_r).alias("second_kwh"),
        F.sum("voltage").over(w_reading_quality).alias("reading_quality_sum_voltage"),
        F.row_number().over(w_reading_quality_site_id_by_r_2).alias("seq_no"),
    )
)

result = (
    sequenced
    .rollup("reading_id", "seq_no")
    .agg(
        F.grouping("reading_id").alias("is_total_reading_id"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("reading_kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("reading_kwh_to_voltage_ratio"),
    )
    .orderBy(F.col("reading_id").asc_nulls_last(), F.col("seq_no").asc_nulls_last())
)
