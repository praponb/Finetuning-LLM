from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc())

base = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.rank().over(w_site_id_quality_by_read_at).alias("read_at_rank"),
        F.lag("read_at", 2).over(w_site_id_quality_by_read_at).alias("prev2_read_at"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.cume_dist().over(w_site_id_quality_by_read_at_t), 3).alias("read_at_cume_dist"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("kwh").alias("total_reading_kwh"))
)

summarized_rows = (
    metrics.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_kwh")
)

screened = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 233.8), "left_semi")
    .filter(F.col("total_reading_kwh").between(216.0, 432.0))
)

result = (
    screened
    .groupingSets(
        [["quality_len", "quality"], ["quality_len"], ["quality"]],
        "quality_len", "quality",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
    .select(
        "quality_len",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "cnt",
        "distinct_qualitys",
    )
    .orderBy(
        F.col("quality_len").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
