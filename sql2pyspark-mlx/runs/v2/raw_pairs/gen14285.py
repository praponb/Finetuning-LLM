from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("budget") <= 2734000) & ((F.col("budget") >= 3661000) | F.col("budget").between(1572000, 4005000)))
    .withColumn("budget_rounded", F.round("budget", 0))
)

summary = (
    filtered_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4291000, "high").when(F.col("budget") >= 1153000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("num_departments"))
)

result = (
    summary
    .select("location", "budget_bucket", "num_departments")
    .orderBy("budget_bucket", "location")
    .limit(5)
)
