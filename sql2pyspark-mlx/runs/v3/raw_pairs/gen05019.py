from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("account_id") == 20) & F.col("merchant").isNotNull()) | (F.col("amount") < 2700))
    .select(
        "id",
        "account_id",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("account_id")
)
