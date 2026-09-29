from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 14, 20]) & F.col("amount").between(2100, 2200) & F.col("account_id").isin([1, 27, 31]))
)
