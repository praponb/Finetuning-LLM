from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & F.col("amount").between(940, 1330))
    .groupBy("method", "status")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
    .filter(F.col("paypal_count") < 62)
    .drop("paypal_count")
    .orderBy("method")
)
