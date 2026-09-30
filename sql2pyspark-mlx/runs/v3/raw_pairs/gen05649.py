from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status").isin(["PENDING", "SHIPPED", "COMPLETED"]) & (F.col("o.status") != "PENDING"))
    .select(
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("channel").desc())
)
