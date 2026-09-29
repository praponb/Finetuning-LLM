from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1900, 2000))
    .select(
        "id",
        "account_id",
        "merchant",
        F.to_date("txn_time").alias("txn_time_date"),
    )
    .orderBy(F.col("id").desc(), "account_id")
    .limit(10)
)
