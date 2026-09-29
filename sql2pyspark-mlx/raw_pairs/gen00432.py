from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "is_active",
        "city",
        F.add_months("signup_date", 1).alias("signup_date_plus_months"),
    )
)
