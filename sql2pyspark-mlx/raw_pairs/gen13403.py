from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-07-01", "2025-02-01") & (F.col("hire_date") <= "2023-08-01"))
    .select(
        "is_remote",
        "full_name",
        "job_title",
        F.when(F.col("job_title").isin(["Intern", "Director"]), "Core").otherwise("Other").alias("job_title_group"),
    )
)
