from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.order_date") > "2024-12-01") & (F.col("p.status") != "FAILED"))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.amount"),
        F.col("p.order_id"),
        F.col("o.order_date"),
        F.col("o.amount").alias("order_amount"),
    )
    .orderBy(F.col("order_amount").desc())
)
