from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull() & (F.col("account_type") == "CREDIT"))
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        "account_type",
        "status",
    )
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") > 40)
)
