from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Shell", "Starbucks"]))
    .select("txn_type")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([9, 10, 39, 40]))
        .select("txn_type")
    )
)
