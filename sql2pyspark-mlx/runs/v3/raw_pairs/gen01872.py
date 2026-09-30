from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.salary"),
        F.col("e.dept_id"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
    .orderBy(F.col("salary").desc(), F.col("dept_name").desc())
)
