from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1300)
    .select("merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("amount").between(1100, 1600))
        .select("merchant", "txn_type")
    )
    .distinct()
    .orderBy("txn_type")
)
