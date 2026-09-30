from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 36)
    .select(
        "id",
        "name",
        "credit_limit",
        "signup_date",
        F.upper("city").alias("city_upper"),
    )
    .orderBy("name", "id")
    .limit(20)
)
