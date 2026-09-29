from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "bank_transfer") | F.col("method").isin(["card", "cash", "promptpay"]))
    .groupBy("method")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 9200)
    .orderBy("method", F.col("total_amount").desc())
)
