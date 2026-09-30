from pyspark.sql import functions as F

max_amount = df_transactions.filter(F.col("account_id").isin([23, 30, 39])).agg(F.max("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") > max_amount)
    .select("id", "amount")
    .orderBy("id")
)
