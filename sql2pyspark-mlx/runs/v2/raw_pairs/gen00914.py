from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "customer_id")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
    .filter(F.col("total_balance") > 198000)
)
