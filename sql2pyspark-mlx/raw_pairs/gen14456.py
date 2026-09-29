from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("c.city") == "Bangkok")
    .select(
        F.col("o.amount"),
        F.col("c.age"),
        F.col("c.city"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.substring(F.col("c.city"), 1, 4).alias("city_prefix"),
    )
    .orderBy("city_prefix")
)
