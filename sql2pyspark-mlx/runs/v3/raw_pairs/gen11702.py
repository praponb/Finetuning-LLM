from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "SG") & (F.col("city") != "Tokyo") & (F.length("email") > 14))
    .select(
        "id",
        "age",
        "signup_date",
        "name",
        F.add_months("signup_date", 12).alias("signup_date_plus_months"),
        F.lower("city").alias("city_lower"),
    )
)
