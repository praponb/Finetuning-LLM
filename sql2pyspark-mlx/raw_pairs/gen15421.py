from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 57, "high").when(F.col("age") >= 37, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count("age").alias("age_count"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy(F.col("stddev_credit_limit").desc())
)
