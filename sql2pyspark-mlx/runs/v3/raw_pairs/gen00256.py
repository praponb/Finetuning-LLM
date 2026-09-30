from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "CREDIT") & ~F.col("merchant").isin(["Amazon", "7-Eleven"]))
    .select("id", "account_id")
)
