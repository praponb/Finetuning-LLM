from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["HR", "Legal", "Sales"]))
    .select(
        F.col("e.is_remote"),
        F.col("e.hire_date"),
        F.col("e.full_name"),
        F.col("d.location"),
        F.concat_ws("-", F.col("d.location"), F.col("d.dept_name")).alias("location_dept_name_key"),
    )
    .orderBy("full_name")
)
