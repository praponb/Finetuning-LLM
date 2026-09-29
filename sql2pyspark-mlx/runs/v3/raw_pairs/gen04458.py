from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2023-12-13")
    .select("job_title")
    .unionByName(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select("job_title")
    )
    .orderBy("job_title")
)
