from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("age") >= 58, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("country") == "SG", F.col("age")).otherwise(0)).alias("sg_age"),
        F.stddev_samp("age").alias("age_stddev"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
    )
)
