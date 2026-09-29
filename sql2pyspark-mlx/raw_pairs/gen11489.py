from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency")
w2 = Window.partitionBy("plan", "currency")
w_part = Window.orderBy(F.col("tier_value").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("currency_min_tier_value").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_min_tier_value").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

enriched = (
    lines
    .select(
        "plan",
        "currency",
        F.min("tier_value").over(w_recent).alias("currency_min_tier_value"),
        F.count("tier_value").over(w2).alias("plan_count_tier_value"),
        F.dense_rank().over(w_part).alias("tier_value_dense_rank"),
    )
)

enriched_price_tiers = (
    enriched
    .withColumn("currency_min_tier_value_dense_rank", F.dense_rank().over(w4))
    .withColumn("currency_min_tier_value_rank", F.rank().over(w5))
)

filtered = (
    enriched_price_tiers
    .filter(F.col("currency_min_tier_value") >= 39)
    .select(
        "plan",
        "currency",
        "currency_min_tier_value",
        "plan_count_tier_value",
        "tier_value_dense_rank",
        "currency_min_tier_value_dense_rank",
        "currency_min_tier_value_rank",
        F.round("currency_min_tier_value_rank", 0).alias("currency_min_tier_value_rank_rounded"),
    )
)

valid = (
    filtered
    .filter(F.col("currency_min_tier_value_dense_rank") == 6)
    .select(
        "currency",
        "plan_count_tier_value",
        "currency_min_tier_value_dense_rank",
        "currency_min_tier_value_rank_rounded",
        (F.col("currency_min_tier_value_rank") + F.col("tier_value_dense_rank")).alias("currency_min_tier_value_rank_plus_tier_value_dense_rank"),
        F.round("tier_value_dense_rank", 1).alias("tier_value_dense_rank_rounded"),
    )
)

grouped = (
    valid
    .withColumn("currency_min_tier_value_rank_plus_tier_value_dense_rank_tier", F.when(F.col("currency_min_tier_value_rank_plus_tier_value_dense_rank") >= 74, "HIGH").otherwise("LOW"))
    .withColumn("tier_value_dense_rank_rounded_tier", F.when(F.col("tier_value_dense_rank_rounded") >= 74, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["currency_min_tier_value_rank_plus_tier_value_dense_rank_tier", "tier_value_dense_rank_rounded_tier"], ["currency_min_tier_value_rank_plus_tier_value_dense_rank_tier"]],
        "currency_min_tier_value_rank_plus_tier_value_dense_rank_tier",
        "tier_value_dense_rank_rounded_tier",
    )
    .agg(
        F.round(F.avg("currency_min_tier_value_rank_rounded"), 2).alias("avg_currency_min_tier_value_rank_rounded"),
        F.round(F.avg("currency_min_tier_value_rank_rounded"), 2).alias("avg_currency_min_tier_value_rank_rounded_2"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.percentile_approx("currency_min_tier_value_rank_rounded", [0.25, 0.5, 0.75]).alias("currency_min_tier_value_rank_rounded_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_currency_min_tier_value_rank_rounded") < 1000000)
)

result = (
    grouped
    .select(
        "currency_min_tier_value_rank_plus_tier_value_dense_rank_tier",
        "tier_value_dense_rank_rounded_tier",
        "avg_currency_min_tier_value_rank_rounded_2",
        "eur_count",
        "currency_min_tier_value_rank_rounded_quartiles",
        "grouping_level",
    )
    .orderBy(
        F.col("avg_currency_min_tier_value_rank_rounded_2").desc(),
        "currency_min_tier_value_rank_plus_tier_value_dense_rank_tier",
    )
)
