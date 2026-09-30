from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") < 33) & (F.col("segment") == "Corporate"))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
    )
)
