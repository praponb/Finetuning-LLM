from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Ivy%"))
    .select(
        "id",
        "segment",
        F.length("email").alias("email_length"),
        F.add_months("signup_date", 6).alias("signup_date_plus_months"),
    )
    .orderBy("signup_date_plus_months", F.col("segment").desc())
)
