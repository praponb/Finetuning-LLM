from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.shipping_fee") < 26)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("c.email"),
        F.col("c.credit_limit"),
        F.trim(F.col("s.city")).alias("clean_city"),
    )
)
