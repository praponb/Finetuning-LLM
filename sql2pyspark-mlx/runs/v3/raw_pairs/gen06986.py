from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([3, 11, 12]) & (F.col("balance") <= 27000))
    .select("account_type")
)
