from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.col("c.age") != 62)
    .select(
        F.col("o.channel"),
        F.col("c.segment"),
        F.col("c.city"),
        F.col("s.city").alias("store_city"),
    )
    .orderBy(F.col("store_city").desc(), F.col("city").desc())
)
