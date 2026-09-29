from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.sum(F.col("o.amount")).alias("total_amount"),
        F.countDistinct(F.col("o.shipping_fee")).alias("unique_customers"),
    )
)
