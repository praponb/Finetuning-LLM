from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("quality") != "OK") & (F.col("site_id") >= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.year(F.col("read_at")).alias("read_at_year"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

annotated = base.withColumn("quality_clean", F.upper(F.trim(F.col("quality"))))

enriched = (
    annotated
    .filter(F.col("voltage") <= 234.7)
    .withColumn("read_at_day", F.date_trunc("day", F.col("read_at")))
)

result = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_prefix",
        "read_at_year",
        "quality_len",
        "quality_clean",
        "read_at_day",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
    .orderBy(F.col("site_id").desc_nulls_first())
)
