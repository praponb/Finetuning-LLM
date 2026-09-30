from pyspark.sql import functions as F

base = df_accounts.filter(F.col("customer_id").isNull() | F.col("customer_id").isNotNull())

accounts_stats = (
    base
    .groupBy(
        F.when(F.col("balance") >= 81000, "high").when(F.col("balance") >= 19000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("*").alias("num_accounts"),
    )
)

result = (
    accounts_stats
    .filter(F.col("num_accounts") >= 5)
    .select("balance_bucket", "min_opened_date", "num_accounts")
    .orderBy("num_accounts")
)
