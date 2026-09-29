from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("value"))

joined = (
    lines
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.id", "src.customer_id", F.col("p.segment"), F.col("p.age"))
)

base = (
    joined
    .filter(F.col("customer_id").isNotNull() & F.col("segment").isNotNull())
    .withColumn("segment_title", F.initcap(F.trim("segment")))
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.customer_id",
        "src.segment",
        "src.segment_title",
        F.col("b.age").alias("customer_age"),
        F.col("b.segment").alias("customer_segment"),
        F.col("b.name"),
    )
)

cleaned = (
    enriched
    .filter(F.col("customer_segment").isNotNull() & (F.col("customer_id") == 23))
    .withColumn("customer_segment_filled", F.coalesce("customer_segment", F.lit("Unknown")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("customer_age") >= 67, "HIGH").otherwise("LOW").alias("customer_age_tier"),
        "customer_id",
    )
    .agg(
        F.size(F.collect_set("customer_segment")).alias("num_customer_segments"),
        F.round(F.sum("customer_age") / F.nullif(F.count(F.when(F.col("customer_segment") == "Consumer", 1)), F.lit(0)), 2).alias("customer_age_per_consumer"),
        F.count(F.when(F.col("customer_segment") == "SMB", 1)).alias("smb_rows"),
    )
)
