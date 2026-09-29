from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("dt.l2_salary"),
        F.col("dt.job_title_values"),
    )
    .orderBy(F.col("budget").desc())
)
