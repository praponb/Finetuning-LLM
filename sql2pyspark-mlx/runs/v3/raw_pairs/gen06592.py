from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
