from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([12, 15, 20, 36]))
    .select("id", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("merchant").isin(["7-Eleven", "Amazon", "Starbucks"]))
        .select("id", "account_id")
    )
    .distinct()
)
