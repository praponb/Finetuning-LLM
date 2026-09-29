from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .select(
        "id",
        "account_id",
        F.concat("txn_type", F.lit("/"), "merchant").alias("txn_type_merchant"),
    )
    .orderBy("id")
)
