from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(740, 1250) & (F.col("paid_at") < "2024-06-08") & (F.col("order_id") == 8))
    .groupBy(F.month("paid_at").alias("paid_month"), "order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
    )
    .filter(F.col("pct_promptpay") > 25)
    .orderBy("pct_promptpay", "paid_month", "order_id")
    .limit(5)
)
