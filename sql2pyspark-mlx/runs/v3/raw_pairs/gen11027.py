from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "name",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy(F.col("id").desc())
)
