from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.orderBy(F.col("plan_avg_tier_value").asc_nulls_last())
w_ordered = Window.orderBy(F.col("is_starter").asc_nulls_first())
w5 = Window.orderBy(F.col("is_starter").desc())
w6 = Window.orderBy(F.col("plan_avg_tier_value").asc_nulls_first())

exploded = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("tier_value"))

valid = (
    exploded
    .filter(F.col("tier_value") < 70)
    .withColumn("is_starter", F.when(F.col("plan") == "starter", 1).otherwise(0))
)

cleaned = (
    valid
    .filter(F.col("tier_value") < 61)
    .select(
        "*",
        F.least("tier_value", "is_starter").alias("least_tier_value_is_starter"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("tier_value") >= 36)
    .withColumn("clean_plan", F.trim("plan"))
)

scored = (
    cleaned_price_tiers
    .filter(~F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "id",
        "tier_value",
        "is_starter",
        F.avg("tier_value").over(w).alias("plan_avg_tier_value"),
    )
)

enriched = (
    scored
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("plan_avg_tier_value_dense_rank", F.dense_rank().over(w3))
)

scored_price_tiers = (
    enriched
    .withColumn("is_starter_rank", F.rank().over(w_ordered))
    .withColumn("is_starter_rank_2", F.rank().over(w5))
    .withColumn("plan_avg_tier_value_percent_rank", F.percent_rank().over(w6))
)

result = (
    scored_price_tiers
    .groupBy(
        F.when(F.col("tier_value") >= 73, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.avg("is_starter").alias("avg_is_starter"),
        F.avg("is_starter_rank").alias("avg_is_starter_rank"),
        F.percentile_approx("is_starter", [0.25, 0.5, 0.75]).alias("is_starter_quartiles"),
    )
    .filter(F.col("avg_is_starter_rank") >= 0)
)
