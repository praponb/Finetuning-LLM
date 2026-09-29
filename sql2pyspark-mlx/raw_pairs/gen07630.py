from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city").isNotNull() & F.month("signup_date").isin([2, 6, 10])) | (F.col("segment") != "Corporate"))
    .select(
        "country",
        F.round((F.col("credit_limit") * 1.07), 2).alias("credit_limit_scaled"),
        F.length("country").alias("country_length"),
    )
    .orderBy(F.col("credit_limit_scaled").desc())
)
