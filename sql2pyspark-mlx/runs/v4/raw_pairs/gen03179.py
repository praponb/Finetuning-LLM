from pyspark.sql import functions as F

result = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.*",
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("o.amount").alias("order_amount"),
    )
)
