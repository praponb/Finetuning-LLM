from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .cube(F.col("p.status").alias("status"), F.col("a.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
