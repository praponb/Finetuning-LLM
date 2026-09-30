from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 1100)
    .select("txn_type", "merchant")
    .intersect(
        df_transactions
        .filter(F.hour("txn_time").between(11, 19))
        .select("txn_type", "merchant")
    )
)
