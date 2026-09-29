from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 40000)
    .select("status", F.lit("account").alias("source"))
    .unionByName(df_payments.select("status", F.lit("payment").alias("source")))
)
