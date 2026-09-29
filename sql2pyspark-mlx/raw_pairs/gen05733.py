from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["US", "UK"]) | (F.col("age") <= 34))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"),
    )
    .orderBy(F.col("age_range").desc())
)
