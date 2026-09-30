from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 2) & F.col("merchant").isin(["7-Eleven", "Grab", "Amazon"]))
    .select(
        "amount",
        F.when(F.col("merchant") == "Grab", 1).otherwise(0).alias("is_grab"),
    )
)
