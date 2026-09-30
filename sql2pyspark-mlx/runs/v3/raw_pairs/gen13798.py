from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select(
        "segment",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
    .orderBy(F.col("segment").desc(), F.col("days_since_signup_date").desc())
)
