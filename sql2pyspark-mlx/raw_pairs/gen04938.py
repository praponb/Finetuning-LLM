from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Amazon"]) | F.col("amount").between(3600, 4000))
    .select(
        "id",
        "account_id",
        F.length("merchant").alias("merchant_length"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
