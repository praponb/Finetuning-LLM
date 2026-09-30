from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.trim(F.col("p.status")).alias("clean_status"),
    )
    .orderBy(F.col("customer_id").desc())
)
