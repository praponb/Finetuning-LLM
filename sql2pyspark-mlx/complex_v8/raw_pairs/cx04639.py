from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 90.6) | (F.col("read_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

stats = (
    prepared_readings
    .groupBy("quality", "quality_len", "site_id")
    .agg(
        F.count(F.when(~F.col("quality").isin("BAD", "EST"), 1)).alias("n_quality_not_in_bad_est"),
        F.max("quality_len").alias("max_quality_len"),
        F.count("*").alias("row_count"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("n_quality_bad"),
    )
)

joined_site = (
    stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.quality",
        "s.quality_len",
        "s.site_id",
        "s.n_quality_not_in_bad_est",
        "s.max_quality_len",
        "s.distinct_site_ids",
        "s.distinct_ids",
        "s.n_quality_bad",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined_site
    .select(
        "quality",
        "quality_len",
        "n_quality_not_in_bad_est",
        "max_quality_len",
        "distinct_site_ids",
        "distinct_ids",
        "site_installed_kw",
        "site_name",
        "site_commissioned_on",
    )
    .orderBy("n_quality_not_in_bad_est", F.col("site_name").desc())
)
