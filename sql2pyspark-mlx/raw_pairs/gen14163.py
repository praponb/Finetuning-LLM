from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["Engineering", "Marketing", "HR"]))
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("e.manager_id"),
        F.col("e.level"),
        F.col("e.dept_id"),
        F.when(F.col("e.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy("level")
)
