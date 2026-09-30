from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "order_id")
    .agg(F.count("*").alias("num_payments"))
    .orderBy("order_id")
)
