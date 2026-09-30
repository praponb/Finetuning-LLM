from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_read_at_to_d = Window.partitionBy("reading_quality", F.to_date("read_at")).orderBy("reading_kwh", "id", "reading_id")
w_quality = Window.partitionBy("quality")
w_reading_quality_read_at_to_d_2 = Window.partitionBy("reading_quality", F.to_date("read_at")).orderBy("reading_kwh", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_plus_7d_to_date = Window.partitionBy(F.to_date("read_at_plus_7d"))

cleaned_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 233.6) & (F.col("kwh").between(15.9, 113.0)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

quality_stats = (
    cleaned_readings
    .groupBy("quality")
    .agg(F.max("kwh").alias("max_kwh"))
)

outliers = (
    cleaned_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

detail_rows = (
    outliers.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_plus_7d",
        "s.kwh_minus_voltage",
        "s.max_kwh",
        "s.kwh_minus_max",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

with_windows = (
    detail_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "read_at_plus_7d",
        "kwh_minus_voltage",
        "kwh_minus_max",
        "reading_id",
        "reading_voltage",
        "reading_quality",
        "reading_read_at",
        "reading_kwh",
        F.lead("voltage").over(w_reading_quality_read_at_to_d).alias("next_voltage"),
        F.sum("max_kwh").over(w_quality).alias("quality_sum_max_kwh"),
        F.round(F.col("kwh_minus_max") / F.nullif(F.sum("kwh_minus_max").over(w_quality), F.lit(0)), 4).alias("kwh_minus_max_share_of_quality"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.nth_value("reading_kwh", 2).over(w_reading_quality_read_at_to_d_2).alias("second_reading_kwh"),
    )
)

qualified = (
    with_windows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

labeled = (
    qualified
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("reading_read_at"))) / 3600, 1).alias("hours_reading_read_at_to_read_at"),
        F.abs(F.col("next_voltage") - 221.8).alias("next_voltage_distance"),
    )
)

with_windows_2 = (
    labeled
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "read_at_plus_7d",
        "kwh_minus_max",
        "reading_id",
        "reading_voltage",
        "reading_quality",
        "reading_read_at",
        "reading_kwh",
        "quality_sum_max_kwh",
        "distinct_site_id_in_quality",
        "hours_reading_read_at_to_read_at",
        "next_voltage_distance",
        F.max("second_reading_kwh").over(w_read_at_plus_7d_to_date).alias("read_at_plus_7d_to_date_max_second_reading_kwh"),
        F.size(F.collect_set("quality").over(w_read_at_plus_7d_to_date)).alias("distinct_quality_in_read_at_plus_7d_to_date"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "read_at",
        "voltage",
        "read_at_plus_7d",
        "kwh_minus_max",
        "reading_id",
        "reading_voltage",
        "reading_quality",
        "quality_sum_max_kwh",
        "distinct_site_id_in_quality",
        "hours_reading_read_at_to_read_at",
        "read_at_plus_7d_to_date_max_second_reading_kwh",
        "distinct_quality_in_read_at_plus_7d_to_date",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.date_format(F.col("reading_read_at"), "MMM yyyy").alias("reading_read_at_label"),
    )
)
