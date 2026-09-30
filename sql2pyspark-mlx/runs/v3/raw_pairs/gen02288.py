from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
