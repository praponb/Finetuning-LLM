from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 30000) | (F.col("customer_id") == 17))
    .orderBy(F.col("id").desc())
    .limit(10)
)
