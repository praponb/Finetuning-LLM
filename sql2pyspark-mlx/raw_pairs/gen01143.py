from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    exploded
    .select(
        "plan",
        "effective_from",
        "element",
        F.when(F.col("plan").rlike("^(enterprise|basic)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("effective_from").asc_nulls_last(), "plan")
)
