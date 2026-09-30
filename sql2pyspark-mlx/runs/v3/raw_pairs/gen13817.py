from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .filter(F.col("o.shipping_fee") < 10)
    .groupBy(F.col("c.country"))
    .agg(
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
        F.min(F.col("o.discount")).alias("min_discount"),
    )
)
