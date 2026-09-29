from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-08-19")
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .pivot("account_type", ["CHECKING", "LOAN", "CREDIT", "SAVINGS"])
    .agg(F.count("balance"))
    .orderBy("opened_month")
)
