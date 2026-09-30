from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 70000)
    .select("id", "customer_id")
    .orderBy(F.col("id").desc(), "customer_id")
)
