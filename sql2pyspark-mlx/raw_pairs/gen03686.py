from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1300, 1700) & F.col("account_id").isin([18, 25, 32, 33]))
    .withColumn("amount_adjusted", F.round((F.col("amount") * 100), 2))
)
