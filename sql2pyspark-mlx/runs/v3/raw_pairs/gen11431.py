from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("c.signup_date"),
        F.col("c.email"),
    )
    .orderBy("sqft")
)
