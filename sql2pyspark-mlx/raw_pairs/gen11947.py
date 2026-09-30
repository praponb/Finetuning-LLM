from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(~F.col("job_title").isin(["Intern", "Analyst"]))
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.pct_designer") > 25)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ebd.pct_designer"),
        F.col("ebd.l1_count"),
    )
)
