from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.min_salary"),
        F.col("ds.level_list"),
    )
    .orderBy(F.col("min_salary").desc())
)
