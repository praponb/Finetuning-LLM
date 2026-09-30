from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") != "Grab")
    .select("txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("merchant").isin(["Amazon", "7-Eleven"]))
        .select("txn_type")
    )
    .distinct()
    .orderBy("txn_type")
)
