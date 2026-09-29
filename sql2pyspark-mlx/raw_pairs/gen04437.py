from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("adjusted_tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("num_plans").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

base = (
    flattened
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-06-19"))) & (F.col("tier_value") >= 75))
    .withColumn("adjusted_tier_value", F.round((F.col("tier_value") * 0.9), 2))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM-dd"))
)

scored = base.withColumn("adjusted_tier_value_rank", F.rank().over(w))

enriched = (
    scored
    .filter(F.col("tier_value") >= 61)
    .select(
        "*",
        F.max("adjusted_tier_value").over(w2).alias("plan_max_adjusted_tier_value"),
    )
)

summary = (
    enriched
    .groupBy(
        "plan",
        F.when(F.col("plan_max_adjusted_tier_value") >= 67, "PREMIUM").otherwise("STANDARD").alias("plan_max_adjusted_tier_value_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .withColumn("num_plans_percent_rank", F.percent_rank().over(w3))
)

result = (
    summary
    .select(
        "plan",
        "plan_max_adjusted_tier_value_tier",
        "num_plans",
        "plan_list",
        "num_plans_percent_rank",
    )
    .orderBy(F.col("plan_max_adjusted_tier_value_tier").desc())
)
