from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 16400, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.countDistinct("city").alias("distinct_cities"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .filter(F.col("distinct_cities") < 1)
    .orderBy("de_events")
)
