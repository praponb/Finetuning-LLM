from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") | (F.col("signup_date") > "2025-03-10"))
    .select(
        "id",
        "signup_date",
        F.length("city").alias("city_length"),
        F.round("age", 1).alias("age_rounded"),
    )
)
