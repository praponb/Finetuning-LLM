from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        F.col("e.dept_id"),
        F.col("e.salary"),
        F.col("e.level"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.when(F.col("d.dept_name") == "Finance", 1).otherwise(0).alias("is_finance"),
    )
)
