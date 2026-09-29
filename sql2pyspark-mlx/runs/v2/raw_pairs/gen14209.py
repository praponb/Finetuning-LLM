from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "is_flagged",
        "account_id",
        F.upper("txn_type").alias("txn_type_upper"),
    )
    .orderBy("id")
    .limit(20)
)
