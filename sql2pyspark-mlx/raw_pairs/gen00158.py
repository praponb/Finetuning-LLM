from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "bob smith")
    .select("hire_date", "job_title")
    .orderBy("job_title")
)
