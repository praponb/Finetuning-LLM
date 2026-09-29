from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("country") != "DE").alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("o.status"),
    )
    .orderBy(F.col("store_id").desc(), F.col("shipping_fee").desc(), "id")
    .limit(10)
)
