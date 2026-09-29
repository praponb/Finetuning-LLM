from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    lines
    .groupBy("product_id")
    .agg(
        F.sum("qty").alias("total_qty"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("price")).otherwise(0)).alias("spring10_price"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.avg("product_id").alias("avg_product_id"),
    )
)
