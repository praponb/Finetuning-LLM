from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("tier_value") >= 85, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "starter", F.col("tier_value")).otherwise(0)).alias("starter_tier_value"),
        F.sum(F.when(F.col("plan") == "starter", F.col("tier_value")).otherwise(0)).alias("starter_tier_value_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.size(F.collect_set("plan")).alias("num_plans_2"),
        F.sum(F.when(F.col("plan") == "starter", F.col("tier_value")).otherwise(0)).alias("starter_tier_value_3"),
    )
    .orderBy(F.col("tier_value_tier").asc_nulls_last())
)
