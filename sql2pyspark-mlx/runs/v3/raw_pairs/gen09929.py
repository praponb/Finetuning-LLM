from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("country").isin(["US", "SG"]) | (F.col("segment") == "SMB")))
    .groupBy(
        F.when(F.col("age") >= 61, "high").when(F.col("age") >= 32, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy(F.col("avg_credit_limit").desc(), "age_bucket")
    .limit(50)
)
