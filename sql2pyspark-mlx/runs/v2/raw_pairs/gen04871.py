from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1900, 3100))
    .orderBy(F.col("id").desc(), F.col("account_id").desc())
)
