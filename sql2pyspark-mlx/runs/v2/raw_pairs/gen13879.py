from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.method").isNotNull())
    .groupBy(F.col("o.channel"))
    .agg(
        F.count("*").alias("num_payments"),
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
    )
)
