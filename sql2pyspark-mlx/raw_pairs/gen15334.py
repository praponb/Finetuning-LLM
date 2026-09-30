from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co") & (F.col("country") == "DE"))
    .select(
        "country",
        "email",
        "credit_limit",
        F.lower("country").alias("country_lower"),
    )
    .orderBy("email", F.col("credit_limit").desc())
)
