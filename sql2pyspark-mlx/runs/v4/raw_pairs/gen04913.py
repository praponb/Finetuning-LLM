from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("hire_date") <= "2023-09-01")
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.total_salary") > 758000)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.level_values"),
        F.col("t.total_salary"),
    )
)
