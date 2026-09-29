from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select(
        "id",
        "credit_limit",
        "name",
        "email",
        F.lower("country").alias("country_lower"),
        F.length("country").alias("country_length"),
    )
)
