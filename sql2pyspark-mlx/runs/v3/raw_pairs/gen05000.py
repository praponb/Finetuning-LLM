from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("level") == "L5")
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("employee_count"),
        F.countDistinct("salary").alias("unique_managers"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("ds.employee_count"),
        F.col("ds.unique_managers"),
    )
    .orderBy(F.col("unique_managers").desc())
)
