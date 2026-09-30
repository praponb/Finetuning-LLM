from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("status") != "FROZEN") & F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
)

agg = (
    recent_accounts
    .groupBy("customer_id", F.year("opened_date").alias("opened_year"))
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    agg
    .filter(F.col("balance_range") < 40)
    .select("customer_id", "opened_year")
    .orderBy("opened_year")
)
