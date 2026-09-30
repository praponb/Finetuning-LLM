from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.date_trunc("year", "signup_date").alias("year_start"),
        F.when(F.col("age") >= 68, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.max(F.when(F.col("segment") == "SMB", F.col("age"))).alias("smb_max_age"),
    )
    .orderBy("chiang_mai_count")
)
