from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("No%"))
    .select(
        "id",
        "signup_date",
        "is_active",
        "city",
        F.greatest("credit_limit", "age").alias("greatest_credit_limit_age"),
    )
    .orderBy("signup_date", "id")
    .limit(20)
)
