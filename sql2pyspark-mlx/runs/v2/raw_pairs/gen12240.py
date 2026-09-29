from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select(
        "id",
        "age",
        "credit_limit",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        F.length("country").alias("country_length"),
    )
    .orderBy(F.col("country_length").desc())
)
