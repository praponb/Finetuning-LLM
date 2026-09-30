from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy("read_at")
w_quality_site_id_by_read_at_p = Window.partitionBy("quality", "site_id").orderBy("read_at", "id").rowsBetween(-2, -1)

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

ranked = (
    scoped_readings
    .select(
        "*",
        F.round(F.cume_dist().over(w_quality_site_id_by_read_at_t), 3).alias("read_at_cume_dist"),
        F.sum("kwh").over(w_quality_site_id_by_read_at_p).alias("kwh_sum_2_prior"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    ranked.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_or_unknown",
        "quality_len",
        "read_at_cume_dist",
        "kwh_sum_2_prior",
        "first_reading_read_at",
        F.concat_ws("|", F.col("quality_or_unknown"), F.col("quality")).alias("quality_or_unknown_quality_label"),
    )
    .orderBy(
        F.col("quality_or_unknown").desc(),
        F.col("read_at_cume_dist").asc_nulls_last(),
    )
)
