from pyspark.sql import functions as F

base = df_customers.filter(F.col("name").like("Pi%") & F.col("email").isNotNull())

segment_summary = (
    base
    .groupBy("segment")
    .agg(
        F.avg("age").alias("avg_age"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
)

result = (
    segment_summary
    .select("segment", "avg_age", "stddev_credit_limit")
    .orderBy("avg_age")
)
