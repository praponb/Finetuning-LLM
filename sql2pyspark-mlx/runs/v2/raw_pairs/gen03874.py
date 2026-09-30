from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Analyst"]))
    .select(
        "hire_date",
        "is_remote",
        "salary",
        F.regexp_replace("full_name", " ", "_").alias("full_name_slug"),
    )
)
