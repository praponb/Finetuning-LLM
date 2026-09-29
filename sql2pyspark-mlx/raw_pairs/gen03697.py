from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 35)
    .select("txn_type", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("amount").isNotNull())
        .select("txn_type", "account_id")
    )
    .distinct()
    .orderBy("txn_type")
)
