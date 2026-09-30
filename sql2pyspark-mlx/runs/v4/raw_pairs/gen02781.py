from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1520, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.count("*").alias("num_rows"),
        F.corr("shipping_fee", "discount").alias("corr_shipping_fee_discount"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy("corr_shipping_fee_discount")
)
