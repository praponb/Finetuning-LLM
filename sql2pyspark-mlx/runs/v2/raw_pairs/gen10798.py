from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 133000) & F.col("job_title").isin(["Analyst", "Designer", "Director"]))
    .select("id", "dept_id")
)
