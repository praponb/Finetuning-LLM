from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Grab", "Amazon"]))
    .select(
        "account_id",
        "txn_time",
        "amount",
        F.when(F.col("merchant") == "Starbucks", 1).otherwise(0).alias("is_starbucks"),
    )
)
