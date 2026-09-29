from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("s.region").isin(["South", "West"]))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("c.city"),
    )
)
