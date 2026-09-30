from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 420) & (F.col("paid_at") >= "2023-08-01") & F.hour("paid_at").between(7, 15))
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("cash_count") > 22)
    .orderBy(F.col("method").desc())
)
