from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.channel") != "mobile")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("c.segment"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("customer_id").desc())
)
