from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
