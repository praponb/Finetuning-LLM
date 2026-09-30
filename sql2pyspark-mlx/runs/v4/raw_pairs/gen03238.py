from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Analyst", "Engineer", "Intern"]) & (F.col("manager_id") == 9))
    .select("id", "email", "hire_date")
)
