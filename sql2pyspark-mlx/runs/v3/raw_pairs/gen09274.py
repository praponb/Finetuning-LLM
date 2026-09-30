from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "New York", "Chiang Mai"]) & F.col("budget").between(1226000, 2501000))
    .withColumn("location_filled", F.coalesce("location", F.lit("Unknown")))
)

location_group_summary = (
    valid_departments
    .groupBy(
        F.when(F.col("location").isin(["Singapore", "Bangkok"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum("budget").alias("total_budget"),
    )
)

result = (
    location_group_summary
    .filter(F.col("stddev_budget") > 44)
    .orderBy(F.col("location_group").desc(), F.col("stddev_budget").desc())
    .limit(1)
)
