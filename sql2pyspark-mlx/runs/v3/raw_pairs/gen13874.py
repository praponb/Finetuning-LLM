from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]) & ((F.col("customer_id") == 35) | F.col("customer_id").isin([20, 29, 39])))
    .withColumn("opened_date_plus_79d", F.date_add("opened_date", 79))
)

balance_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("balance") >= 84000, "high").when(F.col("balance") >= 18000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.count("balance").alias("balance_count"))
)

result = balance_bucket_summary.select("balance_bucket", "balance_count")
