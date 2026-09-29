from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.engineer_salary") > 63)
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.engineer_salary"),
        F.col("ds.level_values"),
    )
    .orderBy("budget")
)
