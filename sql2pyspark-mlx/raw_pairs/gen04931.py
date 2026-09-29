from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("s.sqft") == 7255) | (F.col("o.order_date") >= "2023-05-27"))
    .select(
        F.col("o.discount"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("c.email"),
    )
    .orderBy("email")
)
