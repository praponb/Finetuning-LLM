from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter((F.col("d.dept_name") == "Marketing") | F.col("e.manager_id").isNotNull())
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("e.dept_id"),
        F.col("e.email"),
        F.col("e.manager_id"),
        F.length(F.col("d.dept_name")).alias("dept_name_length"),
    )
)
