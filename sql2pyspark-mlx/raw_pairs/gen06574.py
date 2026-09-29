from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "US") & (F.col("city") == "Berlin"))
    .select(
        "credit_limit",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.lower("country").alias("country_lower"),
    )
)
