from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([7, 13, 20, 28]))
    .select(F.lit("account").alias("origin"), "status")
    .unionByName(df_payments.select(F.lit("payment").alias("origin"), "status"))
)
