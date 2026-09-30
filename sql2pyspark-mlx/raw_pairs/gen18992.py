from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy("account_type")
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc(), "account_type")
    .limit(10)
)
