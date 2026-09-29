from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("budget") < 2229000) & F.col("location").isin(["Berlin", "Bangkok", "Singapore"]) & (F.col("dept_name") == "Legal"))
)

agg = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
    )
)

result = (
    agg
    .select("dept_name", "hr_budget")
    .orderBy("hr_budget")
)
