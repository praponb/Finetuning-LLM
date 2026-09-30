from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("order_date") < "2024-01-01").alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.status"), F.col("p.amount"), F.col("p.method"))
    .orderBy("id")
    .limit(50)
)
