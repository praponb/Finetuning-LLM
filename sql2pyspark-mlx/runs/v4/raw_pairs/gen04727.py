from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

enriched = (
    exploded
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.segment"))
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.email"), F.col("b.city"))
)

filtered = (
    joined
    .filter((F.col("created_at") >= F.lit("2025-03-02 20:00:00").cast("timestamp")) & F.col("value").isin(["WELCOME5", "BUNDLE"]))
    .withColumn("segment_group", F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Priority").otherwise("Other"))
    .withColumn("is_vip", F.when(F.col("value") == "VIP", 1).otherwise(0))
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "created_at",
        "value",
        "segment",
        "city",
        "segment_group",
        "is_vip",
    )
    .orderBy(F.col("id").desc())
)
