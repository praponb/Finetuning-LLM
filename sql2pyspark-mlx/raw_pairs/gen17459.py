from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE"]) & ~F.col("is_flagged"))
    .select("is_flagged", "txn_time", "merchant")
    .orderBy("merchant")
)
