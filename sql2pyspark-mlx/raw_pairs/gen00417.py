from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

result = (
    lines
    .select(
        "plan",
        "effective_from",
        F.when(F.col("plan").rlike("^(pro|enterprise)$"), "PRIORITY").when(F.col("plan").rlike("^(starter|basic)$"), "CORE").otherwise("GROUP_B").alias("plan_zone"),
    )
)
