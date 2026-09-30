from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .filter((F.col("rate") < 5.7) | (F.month(F.col("valid_to")).isin(1, 3)))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
    )
)

per_group = (
    filtered
    .filter(F.col("tariff") != "TOU")
    .groupBy(F.date_trunc("month", "valid_to").alias("valid_to_month"), "tariff")
    .agg(
        F.max("rate").alias("max_rate"),
        F.sum("rate").alias("total_rate"),
        F.min("valid_to").alias("first_valid_to"),
        F.count("*").alias("row_count"),
        F.round(F.sum("rate"), 2).alias("total_rate_2"),
    )
)

annotated = (
    per_group
    .filter(F.col("row_count") < 8)
    .withColumn("total_rate_distance", F.abs(F.col("total_rate") - 22.7))
)

derived = (
    annotated
    .filter(F.col("max_rate") > 2.6)
    .select(
        "valid_to_month",
        "tariff",
        "max_rate",
        "first_valid_to",
        "row_count",
        "total_rate_distance",
        F.coalesce(F.col("total_rate_distance"), F.lit(0.0)).alias("total_rate_distance_filled"),
        F.when(F.col("total_rate_distance") >= 33.6, "HIGH").when(F.col("total_rate_distance") >= 16.8, "MEDIUM").otherwise("LOW").alias("total_rate_distance_tier"),
        F.abs(F.col("row_count") - 8).alias("row_count_distance"),
    )
)

total_rate_distance_tier_stats = (
    derived
    .groupBy("total_rate_distance_tier")
    .agg(F.avg("max_rate").alias("avg_max_rate"))
)

vs_group = (
    derived.alias("s")
    .join(total_rate_distance_tier_stats.alias("g"), F.col("g.total_rate_distance_tier") == F.col("s.total_rate_distance_tier"), "inner")
    .filter(F.col("s.max_rate") > F.col("g.avg_max_rate"))
    .select(
        "s.*",
        "g.avg_max_rate",
        F.round(F.col("s.max_rate") - F.col("g.avg_max_rate"), 2).alias("max_rate_minus_avg"),
    )
)

with_activity = (
    vs_group
    .crossJoin(vs_group.agg(F.avg("max_rate_minus_avg").alias("overall_avg_max_rate_minus_avg")))
    .filter(F.col("max_rate_minus_avg") > F.col("overall_avg_max_rate_minus_avg"))
    .drop("overall_avg_max_rate_minus_avg")
)

result = (
    with_activity
    .groupBy("tariff", "row_count")
    .pivot("total_rate_distance_tier", ["HIGH", "LOW"])
    .agg(F.avg("row_count_distance"))
)
