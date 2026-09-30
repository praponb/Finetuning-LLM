from pyspark.sql import functions as F

min_amount = df_transactions.filter(~F.col("merchant").isin(["7-Eleven", "Amazon"])).agg(F.min("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") == min_amount)
    .select("id", "amount")
    .orderBy(F.col("amount").desc())
)
