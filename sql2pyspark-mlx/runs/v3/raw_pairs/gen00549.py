from pyspark.sql import functions as F

result = (
    df_accounts
    .rollup(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "status")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
)
