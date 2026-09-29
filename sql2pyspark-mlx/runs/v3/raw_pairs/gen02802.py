from pyspark.sql import functions as F

src = (
    df_payments
    .filter(~(F.col("method").isin(["card", "cash"]) & ((F.col("order_id") == 11) | ~F.col("status").isin(["SUCCESS", "FAILED"]))))
)

summary = (
    src
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    summary
    .filter(F.col("total_amount") >= 9300)
    .select("paid_month", "total_amount")
)
