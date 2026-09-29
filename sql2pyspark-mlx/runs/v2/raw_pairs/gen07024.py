from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter((F.col("salary") < 78000) & F.col("dept_id").isin([4, 8, 21, 34]) & (F.col("job_title") == "Engineer"))
)

summary = (
    filtered_employees
    .groupBy("is_remote", F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = summary.orderBy(F.col("first_hire_date").desc())
