from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("dept_id").isin([11, 37]) | F.col("salary").between(114000, 121000))
)

hire_month_summary = (
    recent_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"), "manager_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    hire_month_summary
    .select("hire_month", "manager_id", "level_list", "avg_salary")
    .orderBy(F.col("manager_id").desc())
)
