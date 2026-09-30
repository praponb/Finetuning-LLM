from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 9)
    .select(
        "id",
        "account_id",
        "is_flagged",
        F.when(F.col("txn_type").isin(["CREDIT", "DEBIT"]), "Priority").otherwise("Other").alias("txn_type_group"),
        F.col("amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
