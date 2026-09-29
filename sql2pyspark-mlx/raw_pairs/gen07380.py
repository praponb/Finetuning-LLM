from pyspark.sql import functions as F

filtered_customers = df_customers.filter((F.col("signup_date") > "2025-02-07") & (F.col("age") <= 32))

is_active_summary = (
    filtered_customers
    .groupBy("is_active", "city")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
)

result = (
    is_active_summary
    .filter(F.col("tokyo_count") < 68)
    .select("is_active", "stddev_credit_limit", "tokyo_count")
)
