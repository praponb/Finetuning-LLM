from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([20, 24, 38]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
    .orderBy("pct_savings", "status")
)
