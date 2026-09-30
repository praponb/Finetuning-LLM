from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w_running = Window.partitionBy(F.date_trunc("WEEK", "effective_from"), "plan").orderBy(F.col("tier_value").asc_nulls_first())
w_recent = Window.orderBy(F.col("plan_max_tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

matched_keys = (
    lines
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from").between("2023-11-01", "2024-05-26"))
        .select("id", "plan")
    )
)

verified = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.sum("tier_value").over(w_part).alias("plan_sum_tier_value"),
        F.dense_rank().over(w_running).alias("tier_value_dense_rank"),
        F.max("tier_value").over(w_part).alias("plan_max_tier_value"),
    )
)

cleaned = (
    enriched
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-03-21")))
    .select(
        "*",
        (F.col("plan_sum_tier_value") - F.col("tier_value")).alias("plan_sum_tier_value_minus_tier_value"),
        F.when(F.col("tier_value_dense_rank") >= 8, "High").when(F.col("tier_value_dense_rank") >= 2, "Medium").otherwise("Low").alias("tier_value_dense_rank_band"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter((F.col("effective_from") > F.lit("2024-03-26").cast("date")) & (F.col("tier_value_dense_rank") < 6))
    .select(
        "*",
        F.round((F.col("plan_sum_tier_value_minus_tier_value") * 1.07), 2).alias("adjusted_plan_sum_tier_value_minus_tier_value"),
        (F.col("plan_max_tier_value") - F.col("tier_value_dense_rank")).alias("plan_max_tier_value_minus_tier_value_dense_rank"),
    )
)

scored = (
    cleaned_price_tiers
    .withColumn("plan_min_adjusted_plan_sum_tier_value_minus_tier_value", F.min("adjusted_plan_sum_tier_value_minus_tier_value").over(w_part))
    .withColumn("plan_max_plan_max_tier_value_minus_tier_value_dense_rank", F.max("plan_max_tier_value_minus_tier_value_dense_rank").over(w_part))
    .withColumn("plan_max_tier_value_cume_dist", F.cume_dist().over(w_recent))
)

result = (
    scored
    .select(
        "id",
        "plan",
        "plan_sum_tier_value",
        "tier_value_dense_rank",
        "plan_max_tier_value",
        "plan_sum_tier_value_minus_tier_value",
        "tier_value_dense_rank_band",
        "adjusted_plan_sum_tier_value_minus_tier_value",
        "plan_max_tier_value_minus_tier_value_dense_rank",
    )
    .orderBy(F.col("tier_value_dense_rank").desc())
)
