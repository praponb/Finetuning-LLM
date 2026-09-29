from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .pivot("account_type", ["CHECKING", "LOAN"])
    .agg(F.count("balance"))
)
