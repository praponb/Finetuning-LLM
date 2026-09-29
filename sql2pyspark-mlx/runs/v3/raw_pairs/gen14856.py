from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("status") != "FAILED").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.discount").between(0.18, 0.33))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("customer_id").desc())
)
