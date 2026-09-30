from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Singapore", "Tokyo", "New York"]))
    .select(
        "id",
        "credit_limit",
        "country",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
