from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
    )
)

result = (
    parsed
    .withColumn("weight_kg_tier", F.when(F.col("weight_kg") >= 4.1, "HIGH").otherwise("LOW"))
    .cube("customer_id", "weight_kg_tier")
    .agg(
        F.percentile_approx("weight_kg", [0.25, 0.5, 0.75]).alias("weight_kg_quartiles"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.grouping("customer_id").alias("grouping_level"),
    )
)
