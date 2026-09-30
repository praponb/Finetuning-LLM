from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.year(F.col("s.opened_date")) == 2025)
    .select(
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("c.name"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
    .orderBy("customer_id")
)
