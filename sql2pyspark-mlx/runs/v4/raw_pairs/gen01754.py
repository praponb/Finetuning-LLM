from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.year("hire_date") == 2024)
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.max_salary") > 59000)
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("t.max_salary"),
        F.col("t.level_list"),
    )
    .orderBy(F.col("id").desc())
)
