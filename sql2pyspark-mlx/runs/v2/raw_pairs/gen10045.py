from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 16))
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount").between(1600, 3200))
        .select("txn_type")
    )
    .orderBy(F.col("txn_type").desc())
)
