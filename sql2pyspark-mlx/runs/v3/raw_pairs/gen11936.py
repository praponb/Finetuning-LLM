from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
)
