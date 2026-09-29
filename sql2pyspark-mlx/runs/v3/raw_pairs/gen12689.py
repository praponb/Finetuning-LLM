from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") != "Berlin") & (F.col("age") <= 42))
    .groupBy(
        "country",
        F.when(F.col("credit_limit") >= 16300, "high").when(F.col("credit_limit") >= 3900, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
    )
    .agg(F.max("signup_date").alias("last_signup_date"))
)
