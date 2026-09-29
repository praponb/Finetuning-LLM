from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["DE", "US"]) & (F.length("name") >= 11))
    .groupBy(
        F.when(F.col("credit_limit") >= 15700, "high").when(F.col("credit_limit") >= 2900, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 5)
    .drop("total_count")
    .orderBy("segment_values", "credit_limit_bucket")
    .limit(20)
)
