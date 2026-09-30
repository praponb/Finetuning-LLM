from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("o.order_date"),
        F.col("o.amount").cast("int").alias("amount_int"),
    )
)
