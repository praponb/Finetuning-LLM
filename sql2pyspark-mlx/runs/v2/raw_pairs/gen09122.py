from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("status") == "COMPLETED").alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.status"), F.col("p.order_id"))
    .orderBy("status", F.col("order_id").desc())
)
