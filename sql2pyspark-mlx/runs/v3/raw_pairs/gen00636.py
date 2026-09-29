from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com") & (F.col("signup_date") < "2025-09-01"))
    .groupBy(
        F.when(F.col("credit_limit") >= 12500, "high").when(F.col("credit_limit") >= 2600, "mid").otherwise("low").alias("credit_limit_bucket"),
        "segment",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .filter(F.col("singapore_count") < 2)
)
