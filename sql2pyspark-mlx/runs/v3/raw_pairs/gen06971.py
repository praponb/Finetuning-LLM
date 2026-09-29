from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary")).otherwise(0)).alias("intern_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.intern_salary"),
        F.col("t.job_title_values"),
    )
    .orderBy("job_title_values")
)
