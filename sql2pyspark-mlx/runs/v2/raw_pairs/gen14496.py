from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("name").like("%avi%") & (~F.col("country").isin(["DE", "JP"]) | F.lower("email").like("%ran%")))
    .select(
        "id",
        "credit_limit",
        "country",
        F.date_add("signup_date", 83).alias("signup_date_plus_83d"),
    )
)
