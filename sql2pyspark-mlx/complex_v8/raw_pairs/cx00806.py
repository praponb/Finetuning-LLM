from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

site_id_stats = (
    filtered
    .groupBy("site_id")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

above_average = (
    filtered.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

deduped = (
    above_average
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    deduped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    with_stats
    .cube("site_id", "quality", "reading_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("quality").isin("EST", "OK"), 1)) / F.count("*"), 1).alias("pct_quality_in_est_ok"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("reading_count").asc_nulls_last(),
    )
)
