from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Mia%") & F.col("is_active"))
    .select(
        "id",
        "age",
        "segment",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
