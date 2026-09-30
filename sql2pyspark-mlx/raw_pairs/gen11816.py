from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([3, 4, 11]))
    .select("id", "job_title", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("job_title") == "Intern")
        .select("id", "job_title", "hire_date")
    )
    .distinct()
    .orderBy(F.col("job_title").desc())
)
