from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("order_id", F.trim("method").alias("clean_method"))
    .orderBy("order_id")
)
