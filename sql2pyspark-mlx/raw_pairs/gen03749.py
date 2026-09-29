from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 72000) & ~F.col("account_type").isin(["CREDIT", "CHECKING"]) & (F.col("opened_date") < "2024-06-01"))
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
        F.min("opened_date").alias("first_opened_date"),
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .orderBy(F.col("pct_savings").desc(), "credit_count")
)
