from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(60000, 88000) | F.col("job_title").isin(["Engineer", "Intern"]))
    .select("is_remote", "job_title")
)
