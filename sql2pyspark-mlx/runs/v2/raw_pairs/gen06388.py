from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter((F.col("salary") > 144000) & (F.upper("full_name").like("%ENR%") | (F.col("manager_id") == 32)))
)

hire_month_summary = (
    valid_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    hire_month_summary
    .select("hire_month", "salary_range", "last_hire_date")
    .orderBy(F.col("salary_range").desc())
)
