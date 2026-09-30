from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 3400)
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.col("merchant").isin(["Lazada", "Starbucks", "Shell"]))
        .select("txn_type")
    )
    .orderBy("txn_type")
)
