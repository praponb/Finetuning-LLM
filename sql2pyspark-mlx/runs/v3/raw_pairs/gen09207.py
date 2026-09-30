from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .withColumn("budget_rounded", F.round("budget", 2))
)

dept_name_summary = (
    filtered_departments
    .groupBy("dept_name", "location")
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
    )
)

result = (
    dept_name_summary
    .filter(F.col("new_york_budget") >= 55)
    .select("dept_name", "location", "new_york_budget")
)
