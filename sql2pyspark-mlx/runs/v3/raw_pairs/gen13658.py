from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
    )
    .filter(F.col("card_count") >= 53)
    .drop("card_count")
)
