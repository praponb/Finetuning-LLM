from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter((F.col("d.location") == "Tokyo") & F.col("e.email").like("%@company.co"))
    .select(
        F.col("e.email"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.date_format(F.col("e.hire_date"), "yyyy-MM").alias("hire_date_label"),
    )
    .orderBy(F.col("email").desc())
)
