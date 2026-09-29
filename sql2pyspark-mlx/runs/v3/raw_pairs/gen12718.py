from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([1, 2, 5, 12]))
    .groupBy(
        F.when(F.col("amount") >= 1460, "high").when(F.col("amount") >= 230, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
    .filter(F.col("cash_count") > 8)
    .orderBy(F.col("cash_count").desc(), "amount_bucket")
    .limit(3)
)
