from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("segment") == "SMB") & F.col("email").like("%@yahoo.com") & (F.col("segment") == "Consumer"))
)

city_summary = (
    src
    .groupBy("city")
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

result = (
    city_summary
    .filter(F.col("credit_limit_range") >= 43)
    .select("city", "first_signup_date", "credit_limit_range")
    .orderBy(F.col("credit_limit_range").desc())
)
