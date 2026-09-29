from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 1)
)
