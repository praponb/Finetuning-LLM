from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("o.amount"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("c.name"),
        F.col("c.segment"),
    )
)
