from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "HR"]))
    .withColumn("budget_int", F.col("budget").cast("int"))
)

location_summary = (
    src
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3333000, "high").when(F.col("budget") >= 601000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
    )
)

result = (
    location_summary
    .select("location", "budget_bucket", "engineering_count")
    .orderBy(F.col("engineering_count").desc(), "location", "budget_bucket")
    .limit(20)
)
