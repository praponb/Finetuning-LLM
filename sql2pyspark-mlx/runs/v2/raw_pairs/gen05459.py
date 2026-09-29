from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "CLOSED"]) | (F.col("balance") > 41000))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.max("balance").alias("max_balance"))
    .orderBy(F.col("opened_quarter").desc())
)
