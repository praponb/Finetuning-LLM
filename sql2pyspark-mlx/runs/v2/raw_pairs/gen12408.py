from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([10, 25, 40]))
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
    )
)
