from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2024-12-01")
    .select(
        "id",
        "hire_date",
        "email",
        F.concat("job_title", F.lit("/"), "level").alias("job_title_level"),
    )
    .orderBy("job_title_level", "email")
)
