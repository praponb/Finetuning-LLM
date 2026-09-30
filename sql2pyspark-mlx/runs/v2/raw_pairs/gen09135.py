from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("balance").alias("min_balance"),
        F.count("*").alias("num_accounts"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("min_balance") >= 68000)
    .orderBy("num_accounts")
)
