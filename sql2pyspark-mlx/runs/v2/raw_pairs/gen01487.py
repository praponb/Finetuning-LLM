from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("txn_type").isin(["CREDIT", "DEBIT"]))

summary = (
    filtered_transactions
    .groupBy("merchant")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)

result = summary.orderBy(F.col("unique_accounts").desc())
