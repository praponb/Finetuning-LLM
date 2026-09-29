from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("t.level_list"))
    .orderBy(F.col("dept_name").desc(), F.col("id").desc())
)
