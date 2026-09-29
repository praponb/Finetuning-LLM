from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-02-18", "2024-11-01"))
    .withColumn("balance_scaled", F.round((F.col("balance") * 1.07), 2))
    .orderBy(F.col("account_type").desc())
)
