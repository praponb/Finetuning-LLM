from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-07-03", "2025-05-01") & (F.col("balance") >= 70000) & (F.col("customer_id") == 26))
    .orderBy(F.col("balance").desc())
)
