from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ebd.total_salary"),
        F.col("ebd.level_values"),
    )
    .orderBy("dept_name")
)
