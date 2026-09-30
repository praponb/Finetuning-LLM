from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .withColumn("clean_account_type", F.trim("account_type"))
)

accounts_stats = (
    filtered_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "status")
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
)

result = accounts_stats.select("opened_month", "status", "unique_customers", "pct_open")
