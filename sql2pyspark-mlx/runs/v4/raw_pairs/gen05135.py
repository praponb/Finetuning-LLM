from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([16, 20, 29]) & F.col("customer_id").isNull() & (F.col("balance") < 74000))
    .select("id", "customer_id")
    .orderBy("id", F.col("customer_id").desc())
)
