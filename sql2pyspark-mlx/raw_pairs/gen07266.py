from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([21, 23, 29]) & (F.col("store_id").isNull() | F.col("store_id").isin([7, 15, 24, 29])))
    .select(
        "id",
        "store_id",
        F.date_format("order_date", "MMM yyyy").alias("order_date_label"),
    )
)
