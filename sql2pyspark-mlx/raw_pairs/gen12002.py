from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.status") == "FAILED") | (F.col("p.status") == "FAILED"))
    .select(F.col("o.status"), F.col("p.amount"))
    .orderBy(F.col("amount").desc())
)
