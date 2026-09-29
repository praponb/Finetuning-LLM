from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["FEE", "REFUND"])
    .agg(F.sum("amount"))
)
