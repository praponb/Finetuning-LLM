from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%MMA%") | (F.col("job_title") != "Analyst"))
    .select(
        "hire_date",
        "job_title",
        F.initcap(F.trim("full_name")).alias("full_name_title"),
    )
)
