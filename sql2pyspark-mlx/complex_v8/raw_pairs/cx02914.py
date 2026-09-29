from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality").orderBy(F.col("voltage_minus_max").desc(), F.col("id").desc())

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

annotated = (
    filtered_readings
    .select(
        "id",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

quality_stats = (
    annotated
    .groupBy("quality")
    .agg(F.max("voltage").alias("max_voltage"))
)

above_average = (
    annotated.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

latest_per_key = (
    above_average
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

totals = (
    latest_per_key
    .groupBy(
        "read_at_year",
        F.when(F.col("days_since_read_at") >= 420, "HIGH").when(F.col("days_since_read_at") >= 245, "MEDIUM").otherwise("LOW").alias("days_since_read_at_tier"),
        "quality",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.round(F.sum("days_since_read_at") / F.nullif(F.sum("max_voltage"), F.lit(0)), 3).alias("days_since_read_at_to_max_voltage_ratio"),
        F.sum("read_at_year").alias("sum_read_at_year_check"),
    )
    .filter(F.col("sum_read_at_year_check") > 2024)
    .drop("sum_read_at_year_check")
)

enriched = (
    totals
    .filter(F.col("distinct_qualitys") >= 4)
    .select(
        "read_at_year",
        "days_since_read_at_tier",
        "quality",
        "distinct_ids",
        "distinct_qualitys",
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
    )
)

derived = (
    enriched
    .filter(F.col("distinct_qualitys") > 3)
    .select(
        "*",
        (F.floor(F.col("distinct_qualitys") / 2) * 2).alias("distinct_qualitys_bucket"),
        F.initcap(F.trim(F.col("days_since_read_at_tier"))).alias("days_since_read_at_tier_clean"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

result = (
    derived
    .filter(F.col("quality") == "OK")
    .select(
        "read_at_year",
        "days_since_read_at_tier",
        "distinct_qualitys",
        "distinct_ids_bucket",
        "days_since_read_at_tier_clean",
        "quality_or_unknown",
        (F.col("distinct_qualitys_bucket") - F.col("distinct_ids_bucket")).alias("distinct_qualitys_bucket_minus_distinct_ids_buck"),
    )
    .orderBy(F.col("days_since_read_at_tier_clean").desc())
)
