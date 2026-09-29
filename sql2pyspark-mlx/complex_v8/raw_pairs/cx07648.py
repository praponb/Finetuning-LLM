from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_reading_count_by_max_reading = Window.partitionBy("reading_count").orderBy(F.col("max_reading_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 80.8) & (F.col("site_id") >= 4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.max("voltage").alias("max_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

with_stats = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_voltage",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_details = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.voltage_bucket",
        "s.max_reading_voltage",
        "s.reading_count",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "kwh",
        "quality",
        "max_reading_voltage",
        "reading_count",
        "site_commissioned_on",
        F.round(F.avg("kwh").over(w_quality), 2).alias("quality_avg_kwh"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.sum("reading_count").over(w_reading_count_by_max_reading).alias("running_reading_count"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.sum("reading_count").over(w_quality).alias("quality_sum_reading_count"),
    )
)

result = (
    with_windows
    .filter(F.col("reading_count") <= 6)
    .select(
        "id",
        "kwh",
        "max_reading_voltage",
        "reading_count",
        "quality_avg_kwh",
        "rows_in_quality",
        "running_reading_count",
        "distinct_site_id_in_quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
    .orderBy(
        F.col("max_reading_voltage").desc(),
        F.col("rows_in_quality").desc_nulls_first(),
    )
)
