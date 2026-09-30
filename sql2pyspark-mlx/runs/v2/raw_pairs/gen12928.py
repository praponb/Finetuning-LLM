from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location").isin(["Singapore", "Bangkok"]))
    .select(
        F.col("d.location"),
        F.col("e.dept_id"),
        F.col("e.email"),
        F.col("e.is_remote"),
    )
)
