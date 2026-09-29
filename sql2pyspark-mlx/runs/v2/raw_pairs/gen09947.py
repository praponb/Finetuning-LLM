from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isin(["North", "South", "Central"]))
    .select(
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("s.store_name"),
    )
)
