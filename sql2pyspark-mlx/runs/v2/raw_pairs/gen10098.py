from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .select(
        "id",
        "segment",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy("is_active_label")
)
