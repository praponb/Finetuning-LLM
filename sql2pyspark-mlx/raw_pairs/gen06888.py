from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

result = (
    exploded
    .select(
        "id",
        "plan",
        "val",
        F.when(F.col("plan").rlike("^(basic|pro)$"), "GROUP_A").when(F.col("plan").rlike("^(starter|enterprise)$"), "EXTENDED").otherwise("CORE").alias("plan_zone"),
    )
)
