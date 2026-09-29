from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & F.col("name").like("No%") & (F.length("email") > 8))
    .select(
        "id",
        "credit_limit",
        "name",
        F.date_format("signup_date", "yyyy-MM").alias("signup_date_label"),
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
    .orderBy("name", "id")
    .limit(20)
)
