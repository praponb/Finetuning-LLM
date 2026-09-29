from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-08-31", "2025-07-01"))
    .select(
        "name",
        "city",
        F.concat("city", F.lit(" "), "email").alias("city_email"),
        F.trim("email").alias("clean_email"),
    )
)
