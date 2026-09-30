from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .withColumn("age_tier", F.when(F.col("age") >= 73, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "age_tier", "city"], ["quarter_start", "age_tier"]],
        "quarter_start",
        "age_tier",
        "city",
    )
    .agg(
        F.min("age").alias("min_age"),
        F.stddev_samp("age").alias("age_stddev"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .filter(F.col("min_age") >= 0)
    .orderBy(F.col("age_stddev").desc())
)
