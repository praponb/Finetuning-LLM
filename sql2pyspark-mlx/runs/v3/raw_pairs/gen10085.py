from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 9, 10]))
    .groupBy(
        "segment",
        F.when(F.col("credit_limit") >= 16900, "high").when(F.col("credit_limit") >= 8100, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.when(F.col("age") >= 62, "high").when(F.col("age") >= 34, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
)
