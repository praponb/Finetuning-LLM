from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("c.city") == "Chiang Mai")
    .select(
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("s.sqft"),
    )
)
