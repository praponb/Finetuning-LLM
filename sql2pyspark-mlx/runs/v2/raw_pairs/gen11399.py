from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("amount") < 3700)

txn_month_summary = (
    recent_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
)

result = (
    txn_month_summary
    .filter(F.col("pct_shell") >= 37)
    .select("txn_month", "pct_shell")
    .orderBy("txn_month")
    .limit(50)
)
