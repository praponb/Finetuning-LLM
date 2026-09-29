from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Engineering"]))
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"),
    )
    .orderBy("hr_count", "dept_name")
    .limit(20)
)
