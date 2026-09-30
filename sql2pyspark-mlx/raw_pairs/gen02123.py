from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "credit_limit",
        "email",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
