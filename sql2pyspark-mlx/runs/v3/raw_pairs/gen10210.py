from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("location").isin(["New York", "Tokyo", "Chiang Mai"]))

dept_name_summary = (
    recent_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
    )
)

result = (
    dept_name_summary
    .filter(F.col("new_york_budget") < 32)
    .orderBy(F.col("new_york_budget").desc(), "dept_name")
    .limit(20)
)
