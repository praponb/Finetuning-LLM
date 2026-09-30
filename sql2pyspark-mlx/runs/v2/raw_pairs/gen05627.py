from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("budget") > 1967000) & (F.col("budget") < 2806000) & (F.col("location") != "Singapore"))
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
)

departments_stats = (
    base
    .groupBy("dept_name")
    .agg(
        F.max("budget").alias("max_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = (
    departments_stats
    .filter(F.col("avg_budget") >= 42)
    .orderBy("max_budget", F.col("avg_budget").desc(), "dept_name")
    .limit(50)
)
