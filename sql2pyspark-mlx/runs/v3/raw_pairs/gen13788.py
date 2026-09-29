from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("dept_name").isin(["HR", "Marketing"]))

dept_name_summary = (
    recent_departments
    .groupBy("dept_name")
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)

result = dept_name_summary.orderBy("dept_name", "stddev_budget")
