from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
    .filter(F.col("hr_count") > 7)
    .orderBy(F.col("dept_name").desc(), F.col("avg_budget").desc())
    .limit(5)
)
