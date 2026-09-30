from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)
