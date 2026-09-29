from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

result = (
    flattened
    .select(
        "plan",
        "element",
        F.when(F.col("plan").rlike("^(pro|starter)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
)
