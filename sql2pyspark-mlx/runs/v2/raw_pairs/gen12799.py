from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([6, 8, 11]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(df_payments.select("status", F.lit("payment").alias("source")))
    .orderBy(F.col("status").desc())
)
