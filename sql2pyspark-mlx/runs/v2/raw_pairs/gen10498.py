from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 17, 23]))
    .select("account_id")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type").isin(["CREDIT", "DEBIT"]))
        .select("account_id")
    )
    .distinct()
    .orderBy("account_id")
)
