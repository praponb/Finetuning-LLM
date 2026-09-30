from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Alice%") & F.col("name").isNotNull() & (F.col("credit_limit") <= 11200))
    .select(
        "id",
        "segment",
        "signup_date",
        "country",
        F.trim("city").alias("clean_city"),
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
    )
    .orderBy("segment", F.col("country").desc())
)
