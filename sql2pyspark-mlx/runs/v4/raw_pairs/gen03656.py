from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-10-09"))) & F.col("level").isNotNull() & F.col("dept_id").isin([12, 35, 39]))
    .select("*", F.round((F.col("salary") * 1.15), 2).alias("salary_scaled"))
)

result = (
    filtered
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.level").alias("employee_level"),
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)
