from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("paid_at") >= "2024-09-25") & ~F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .withColumn("method_length", F.length("method"))
)

summary = (
    filtered_payments
    .groupBy("status")
    .agg(F.max("method_length").alias("max_method_length"))
)

result = summary.select("status", "max_method_length")
