from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["cash", "paypal", "bank_transfer"]) | F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .groupBy("status", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
