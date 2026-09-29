from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.location").isin(["Tokyo", "New York"]))
    .select(
        F.col("d.location"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("e.salary"),
    )
    .orderBy("salary")
)
