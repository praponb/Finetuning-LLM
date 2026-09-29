from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "card")
    .select("amount", "order_id", "method", F.trim("method").alias("clean_method"))
    .orderBy("clean_method", F.col("method").desc())
)
