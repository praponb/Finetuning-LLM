from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([9, 14, 18, 21]) & F.col("merchant").isin(["Grab", "7-Eleven", "Amazon"]) & (F.col("amount") < 2900))
    .select(
        "id",
        "merchant",
        "amount",
        "is_flagged",
        F.length("txn_type").alias("txn_type_length"),
        F.concat("txn_type", F.lit(", "), "merchant").alias("txn_type_merchant"),
    )
)
