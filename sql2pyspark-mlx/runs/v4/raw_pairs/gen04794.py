from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1700, 2500) & F.col("merchant").isNull() & (F.col("merchant") != "Lazada"))
    .select(
        "id",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.concat("txn_type", F.lit(" - "), "merchant").alias("txn_type_merchant"),
    )
)
