from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .select("merchant")
    .unionByName(
        df_transactions
        .filter(F.col("amount").between(2400, 3300))
        .select("merchant")
    )
    .distinct()
    .orderBy("merchant")
)
