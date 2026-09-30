from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.max(F.when(F.col("channel") == "partner", F.col("amount"))).alias("partner_max_amount"),
        F.sum(F.when(F.col("channel") == "web", F.col("shipping_fee")).otherwise(0)).alias("web_shipping_fee"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("shipping_fee")).otherwise(0)).alias("pending_shipping_fee"),
    )
    .filter(F.col("pending_shipping_fee") > 0)
)
