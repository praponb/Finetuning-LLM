from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 45000)
    .select("id", "balance", "customer_id")
    .orderBy(F.col("balance").desc())
)
