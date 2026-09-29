from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter(F.col("p.status") == "FAILED")
    .select(F.col("p.id"), F.col("p.order_id"), F.col("o.store_id"), F.col("o.status"))
    .orderBy(F.col("id").desc())
)
