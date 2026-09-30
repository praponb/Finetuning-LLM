from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(~F.col("d.dept_name").isin(["Engineering", "Sales"]))
    .select(
        F.col("e.hire_date"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.upper(F.col("d.location")).alias("location_upper"),
    )
)
