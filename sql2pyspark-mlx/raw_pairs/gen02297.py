from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.shipping_fee").between(19, 23))
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("o.shipping_fee")).alias("shipping_fee_count"),
        F.countDistinct(F.col("o.shipping_fee")).alias("unique_customers"),
    )
)
