from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("tier_value")
w2 = Window.partitionBy(F.date_trunc("YEAR", "effective_from")).orderBy(F.col("tier_value").desc_nulls_first())
w_part = Window.partitionBy(F.date_trunc("week", "effective_from"), "plan").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan")
w_grp = Window.orderBy(F.col("tier_value_per_tier_value_cume_dist").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.cume_dist().over(w_part).alias("tier_value_cume_dist"),
        F.avg("tier_value").over(w4).alias("plan_avg_tier_value"),
    )
)

base = (
    metrics
    .filter((F.col("effective_from") >= F.lit("2024-02-12").cast("date")) & (F.col("plan") == "pro"))
    .select(
        "id",
        "tier_value",
        "tier_value_dense_rank",
        "tier_value_rank",
        "tier_value_cume_dist",
        "plan_avg_tier_value",
        (F.datediff(F.current_date(), "effective_from") / 365.25).alias("years_since_effective_from"),
        (F.col("tier_value") / F.nullif("tier_value_cume_dist", F.lit(0))).alias("tier_value_per_tier_value_cume_dist"),
    )
)

valid = (
    base
    .filter(F.col("years_since_effective_from") > 38)
    .select(
        "tier_value",
        "plan_avg_tier_value",
        "years_since_effective_from",
        "tier_value_per_tier_value_cume_dist",
        F.round("tier_value_per_tier_value_cume_dist", 1).alias("tier_value_per_tier_value_cume_dist_rounded"),
    )
)

metrics_price_tiers = (
    valid
    .filter(F.col("tier_value_per_tier_value_cume_dist").isNotNull())
    .withColumn("tier_value_per_tier_value_cume_dist_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("tier_value_per_tier_value_cume_dist_rank", F.rank().over(w_grp))
)

result = (
    metrics_price_tiers
    .cube(
        F.when(F.col("tier_value_per_tier_value_cume_dist_rounded") >= 76, "HIGH").otherwise("LOW").alias("tier_value_per_tier_value_cume_dist_rounded_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("tier_value_per_tier_value_cume_dist"), 2).alias("avg_tier_value_per_tier_value_cume_dist"),
        F.round(F.avg("tier_value_per_tier_value_cume_dist_rounded"), 2).alias("avg_tier_value_per_tier_value_cume_dist_rounded"),
    )
    .filter(F.col("avg_tier_value_per_tier_value_cume_dist") > 0)
    .orderBy("avg_tier_value_per_tier_value_cume_dist_rounded")
)
