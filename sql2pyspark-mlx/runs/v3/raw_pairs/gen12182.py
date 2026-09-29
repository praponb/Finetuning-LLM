from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("location") == "Tokyo") & ((F.col("dept_name") == "HR") | F.col("location").isin(["Berlin", "London"])))
)

dept_name_summary = (
    filtered_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4410000, "high").when(F.col("budget") >= 1412000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
        F.count("*").alias("department_count"),
    )
)

result = (
    dept_name_summary
    .select("dept_name", "budget_bucket", "engineering_budget")
    .orderBy("engineering_budget", "dept_name", "budget_bucket")
    .limit(3)
)
