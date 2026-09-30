from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount") >= 1360)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("id").desc(), "store_name")
)
