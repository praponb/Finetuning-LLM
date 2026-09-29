from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.year("signup_date") >= 2025))
    .select(
        "id",
        "email",
        "country",
        F.when(F.col("credit_limit") >= 16600, "High").when(F.col("credit_limit") >= 2500, "Medium").otherwise("Low").alias("credit_limit_tier"),
    )
    .orderBy(F.col("email").desc())
)
