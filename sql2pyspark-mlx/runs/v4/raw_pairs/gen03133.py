from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isNull())
    .select(
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("order_amount").desc())
)
