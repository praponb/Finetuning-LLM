from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1000, 1500) & F.col("merchant").isNull() & F.col("merchant").isin(["Grab", "7-Eleven"]))
    .select(
        "txn_time",
        F.when(F.col("merchant") == "Shell", 1).otherwise(0).alias("is_shell"),
    )
    .orderBy(F.col("is_shell").desc())
)
