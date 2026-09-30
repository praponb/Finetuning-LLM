from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

result = (
    lines
    .select(
        "id",
        "plan",
        "currency",
        F.when(F.col("plan").rlike("^(enterprise|pro)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("plan_zone"),
    )
)
