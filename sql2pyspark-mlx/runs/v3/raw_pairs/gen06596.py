from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("age") >= 38) & ~F.col("email").like("%@startup.io"))
    .withColumn("age_whole", F.floor("age"))
)

summary = (
    src
    .groupBy("is_active")
    .agg(
        F.round(F.avg("age_whole"), 2).alias("avg_age_whole"),
        F.round(F.stddev("age_whole"), 2).alias("stddev_age_whole"),
    )
)

result = (
    summary
    .filter(F.col("stddev_age_whole") >= 70)
    .orderBy(F.col("stddev_age_whole").desc(), F.col("avg_age_whole").desc())
)
