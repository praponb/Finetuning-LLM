from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Consumer"]) | (F.col("signup_date") < "2025-08-29"))
    .select(
        "id",
        "name",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
        F.lower("name").alias("name_lower"),
    )
    .orderBy(F.col("id").desc())
)
