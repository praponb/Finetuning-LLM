from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") < 318000)
    .orderBy(F.col("opened_month").desc())
)
