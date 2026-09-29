from pyspark.sql import functions as F

min_amount = df_transactions.filter(~F.col("is_flagged")).agg(F.min("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") <= min_amount)
    .select("id", "amount", "account_id")
    .orderBy(F.col("amount").desc())
)
