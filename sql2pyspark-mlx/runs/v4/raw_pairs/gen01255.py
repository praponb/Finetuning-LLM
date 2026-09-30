from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("budget") <= 3770000)
    .withColumn("location_group", F.when(F.col("location").isin(["Tokyo", "Bangkok"]), "Core").otherwise("Other"))
)

summary = (
    filtered_departments
    .groupBy("location")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = (
    summary
    .filter(F.col("stddev_budget") > 27)
    .orderBy(F.col("avg_budget").desc(), "location")
    .limit(50)
)
