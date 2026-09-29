from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.count("*").alias("num_accounts"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
        F.min("opened_date").alias("first_opened_date"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") > 378000)
    .drop("total_balance")
    .orderBy("first_opened_date", "account_type")
    .limit(5)
)
