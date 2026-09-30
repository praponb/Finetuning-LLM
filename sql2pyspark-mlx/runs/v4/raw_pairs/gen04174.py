from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("level") == "L5")
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("dt.salary_range"),
        F.col("dt.min_hire_date"),
    )
    .orderBy(F.col("location").desc())
)
