from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "7-Eleven", "Grab"]))
    .select("txn_type")
    .unionByName(
        df_transactions
        .filter(F.to_date("txn_time") >= "2024-10-01")
        .select("txn_type")
    )
    .distinct()
    .orderBy("txn_type")
)
