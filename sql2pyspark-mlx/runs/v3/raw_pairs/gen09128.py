from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("status")
    .agg(F.count("*").alias("num_accounts"))
    .filter(F.col("num_accounts") >= 4)
    .select("status", "num_accounts")
    .orderBy("status")
)
