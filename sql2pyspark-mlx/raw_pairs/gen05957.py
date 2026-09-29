from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("kind"), "status")
    .unionByName(df_payments.select(F.lit("payment").alias("kind"), "status"))
    .orderBy("status")
)
