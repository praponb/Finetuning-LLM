from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("year_start", F.date_trunc("YEAR", "signup_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .rollup("year_start", "week_start")
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("credit_limit_per_smb"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.grouping_id().alias("grouping_level"),
    )
)
