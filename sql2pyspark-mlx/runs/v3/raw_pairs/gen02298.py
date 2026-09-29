from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 1050)
    .groupBy("status", "method")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
    )
    .filter(F.col("avg_amount") < 1380)
    .orderBy(F.col("promptpay_amount").desc(), "status", "method")
    .limit(10)
)
