from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .select("txn_type", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type") == "CREDIT")
        .select("txn_type", "merchant")
    )
    .distinct()
    .orderBy("merchant")
)
