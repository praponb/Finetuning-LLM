from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("merchant")
    .intersect(df_transactions.filter(F.col("account_id") == 18).select("merchant"))
)
