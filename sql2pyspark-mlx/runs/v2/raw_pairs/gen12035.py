from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.sqft") <= 6493) & F.col("s.store_name").like("Mal%"))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("s.city"),
    )
)
