from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("clean_location", F.trim("location"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("N/A")))
)

result = (
    base
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "London"])
    .agg(F.avg("budget").alias("total"), F.count(F.lit(1)).alias("n"))
)
