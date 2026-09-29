from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
    )
)
