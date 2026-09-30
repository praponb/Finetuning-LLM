from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1400, 2500))
    .select("merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("merchant") == "Grab")
        .select("merchant", "txn_type")
    )
)
