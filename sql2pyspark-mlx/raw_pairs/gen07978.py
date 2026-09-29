from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.promo"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("product_id") >= 11, "PREMIUM").otherwise("STANDARD").alias("product_id_tier"),
    )
    .agg(
        F.avg("product_id").alias("avg_product_id"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.sum(F.when(F.col("promo") == "VIP", F.col("product_id")).otherwise(0)).alias("vip_product_id"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
    )
)
