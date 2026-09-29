from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("o.channel") == "store", F.col("o.discount")).otherwise(0)).alias("store_discount"),
        F.sum(F.when(F.col("o.status") == "SHIPPED", F.col("o.shipping_fee")).otherwise(0)).alias("shipped_shipping_fee"),
    )
    .filter(F.col("store_discount") >= 29)
    .orderBy(F.col("segment").desc(), F.col("shipped_shipping_fee").desc())
)
