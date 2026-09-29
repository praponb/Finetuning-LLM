from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "status")
    .agg(F.count("*").alias("num_accounts"))
)
