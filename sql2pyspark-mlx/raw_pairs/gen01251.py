from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.discount") >= 0.27)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
)
