from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("salary") >= 148000)
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(F.col("d.dept_name"), F.col("ds.first_hire_date"), F.col("ds.l3_count"))
)
