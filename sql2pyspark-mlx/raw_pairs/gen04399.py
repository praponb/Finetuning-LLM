from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Shell") & F.hour("txn_time").between(12, 20))
    .groupBy(
        F.to_date("txn_time").alias("txn_day"),
        F.when(F.col("amount") >= 3500, "high").when(F.col("amount") >= 1600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("transaction_count"))
    .filter(F.col("transaction_count") < 2)
    .orderBy("txn_day")
)
