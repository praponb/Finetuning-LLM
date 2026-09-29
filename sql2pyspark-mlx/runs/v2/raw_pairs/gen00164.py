from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("manager_id") == 4)
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ebd.job_title_list"),
        F.col("ebd.level_list"),
    )
)
