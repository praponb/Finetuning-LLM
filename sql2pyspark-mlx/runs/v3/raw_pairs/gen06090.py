from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount").between(1200, 3000) & (F.to_date("txn_time") >= "2023-11-01")) | (F.col("txn_type") != "DEBIT"))
    .select(
        "id",
        "txn_time",
        "amount",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy(F.col("id").desc(), "is_flagged_label")
)
