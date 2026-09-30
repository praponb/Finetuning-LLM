from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "email",
        "country",
        "city",
        F.add_months("signup_date", 6).alias("signup_date_plus_months"),
        F.coalesce("age", F.lit(0)).alias("age_filled"),
    )
)
