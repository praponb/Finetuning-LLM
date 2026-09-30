from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .cube(
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .orderBy(F.col("signup_month").desc())
)
