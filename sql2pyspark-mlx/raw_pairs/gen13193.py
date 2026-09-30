from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.order_id").isin([17, 33]) | F.col("p.order_id").isNull())
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.status").alias("order_status"),
    )
    .orderBy(F.col("order_id").desc(), "id")
    .limit(5)
)
