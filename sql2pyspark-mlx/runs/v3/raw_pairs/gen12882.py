from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 29) & (F.col("status") == "REFUNDED"))
    .groupBy("order_id", "status")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status", "status_values")
)
