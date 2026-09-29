from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "DEBIT"]))
    .select("account_id")
    .intersect(
        df_transactions
        .filter(F.col("amount").between(1300, 1600))
        .select("account_id")
    )
    .orderBy("account_id")
)
