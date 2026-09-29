from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("country").isin(["US", "SG"]) | (F.col("segment") == "SMB")))
    .groupBy(
        F.when(F.col("credit_limit") >= 15200, "high").when(F.col("credit_limit") >= 4100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy(F.col("avg_credit_limit").desc(), "credit_limit_bucket")
    .limit(50)
)
