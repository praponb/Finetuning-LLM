from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(66000, 67000) & (F.col("account_type") == "CHECKING") & F.col("status").isin(["OPEN", "CLOSED"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") < 52)
    .orderBy(F.col("balance_range").desc())
)
