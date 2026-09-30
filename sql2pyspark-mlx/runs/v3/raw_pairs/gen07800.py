from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.sum(F.when(F.col("channel") == "partner", F.col("amount")).otherwise(0)).alias("partner_amount"),
    )
    .filter(F.col("total_shipping_fee") > 130)
    .orderBy(F.col("total_shipping_fee").desc(), F.col("customer_id").desc())
)
