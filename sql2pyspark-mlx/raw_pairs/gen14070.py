from pyspark.sql import functions as F

result = (
    df_payments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.status",
        F.col("b.amount").alias("order_amount"),
        F.col("b.discount"),
    )
)
