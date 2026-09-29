from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3200, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 53)
    .select("amount_bucket", "txn_type_values", "avg_amount")
    .orderBy("amount_bucket", F.col("avg_amount").desc())
    .limit(10)
)
