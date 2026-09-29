from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("order_id").isin([10, 14, 39]) | F.col("order_id").isin([1, 17, 33, 38]))
)

method_summary = (
    src
    .groupBy("method", "status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = method_summary.select("method", "status", "amount_range")
