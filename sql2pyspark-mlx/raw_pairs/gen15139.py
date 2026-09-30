from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

filtered = (
    exploded
    .filter(F.col("element") == "VIP")
    .withColumn("element_lower", F.lower("element"))
    .withColumn("element_group", F.when(F.col("element").isin(["VIP", "BUNDLE"]), "Group A").otherwise("Other"))
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.customer_id",
        "e.element",
        "e.element_lower",
        "e.element_group",
        F.col("p.segment"),
        F.col("p.country"),
        F.col("p.city"),
    )
)

base = (
    joined
    .filter(F.col("city").isNotNull() & (F.col("element").eqNullSafe(F.lit("WELCOME5"))))
    .select(
        "element_lower",
        "element_group",
        "country",
        "city",
        F.when(F.col("element") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("is_welcome5") >= 84, "PREMIUM").otherwise("STANDARD").alias("is_welcome5_tier"),
    )
    .agg(
        F.round(F.sum("is_welcome5") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("is_welcome5_per_bangkok"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
)
