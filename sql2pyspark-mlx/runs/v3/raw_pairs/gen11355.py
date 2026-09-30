from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "LOAN"]))

summary = (
    filtered_accounts
    .groupBy(
        F.when(F.col("balance") >= 65000, "high").when(F.col("balance") >= 31000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.max("opened_date").alias("latest_opened_date"))
)

result = (
    summary
    .select("balance_bucket", "latest_opened_date")
    .orderBy("balance_bucket")
)
