from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNull() & (F.col("credit_limit") >= 8600) & ~F.col("is_active"))
    .select(
        "id",
        "email",
        F.concat("country", F.lit("/"), "segment").alias("country_segment"),
        F.lower("city").alias("city_lower"),
    )
)
