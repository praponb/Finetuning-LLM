from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("element"))

result = (
    lines
    .select(
        "id",
        "plan",
        "element",
        F.when(F.col("plan").rlike("^(starter|enterprise)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("element").desc())
)
