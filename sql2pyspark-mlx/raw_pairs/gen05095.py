from pyspark.sql import functions as F

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount") <= 0.1)
    .select(
        "src.*",
        F.col("o.amount").alias("order_amount"),
        F.col("o.order_date"),
        F.col("o.channel"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "order_amount",
        "order_date",
        "channel",
    )
    .orderBy(F.col("status").desc())
)
