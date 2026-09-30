from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 3700) & F.col("amount").isNull())
    .select("id", "account_id", "merchant")
    .orderBy(F.col("merchant").desc(), F.col("account_id").desc())
)
