from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.to_date("paid_at").alias("paid_day"), "order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .filter(F.col("avg_amount") >= 1030)
)
