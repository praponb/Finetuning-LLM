from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L4", "L5"]) & ((F.col("hire_date") <= "2024-09-04") | (F.col("job_title") != "Analyst")))
    .select("id", "dept_id", "full_name")
)
