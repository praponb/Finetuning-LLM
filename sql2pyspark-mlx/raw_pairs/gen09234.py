from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("e.level") == "L5") & ~F.col("d.location").isin(["Chiang Mai", "Singapore"]))
    .select(
        F.col("e.id"),
        F.col("e.hire_date"),
        F.col("e.full_name"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
)
