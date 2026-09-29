from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.trim(F.col("o.status")).alias("clean_status"),
    )
    .orderBy(F.col("amount").desc())
)
