from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([2, 3, 4]) & ~F.col("name").like("%Chen%") & ~F.col("city").isin(["Bangkok", "New York"]))
    .select(
        "id",
        "credit_limit",
        "is_active",
        "segment",
        F.concat("country", F.lit("/"), "segment").alias("country_segment"),
        F.substring("segment", 1, 2).alias("segment_prefix"),
    )
    .orderBy(F.col("id").desc(), "segment_prefix")
    .limit(20)
)
