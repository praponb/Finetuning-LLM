from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("c.credit_limit"),
        F.col("c.email"),
        F.round(F.col("c.credit_limit"), 1).alias("credit_limit_rounded"),
    )
)
