from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("budget") <= 1847000) | F.col("budget").between(1733000, 1845000))
)

dept_name_summary = (
    valid_departments
    .groupBy("dept_name")
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
)

result = (
    dept_name_summary
    .filter(F.col("avg_budget") > 36)
    .select("dept_name", "avg_budget")
    .orderBy(F.col("dept_name").desc())
    .limit(5)
)
