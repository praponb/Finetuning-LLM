from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2025-03-01")
    .select("account_id")
    .unionByName(
        df_transactions
        .filter(F.col("merchant").isin(["Starbucks", "Grab"]))
        .select("account_id")
    )
    .distinct()
)
