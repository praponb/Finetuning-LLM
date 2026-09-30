from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("val"))

result = (
    exploded
    .select(
        "id",
        "plan",
        "val",
        F.when(F.col("plan").rlike("^(enterprise|starter)$"), "PRIORITY").when(F.col("plan").rlike("^(basic|pro)$"), "GROUP_B").otherwise("EXTENDED").alias("plan_zone"),
    )
    .orderBy(F.col("id").desc())
)
