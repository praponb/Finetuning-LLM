from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "is_active",
        F.when(F.col("credit_limit") >= 15800, "high").when(F.col("credit_limit") >= 7200, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
    )
    .filter(F.col("us_count") < 50)
    .select("is_active", "credit_limit_bucket", "country_values", "us_count")
    .orderBy(F.col("us_count").desc())
)
