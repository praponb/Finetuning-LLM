from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Designer", "Intern"]))
    .select("id", "email")
    .orderBy("id")
    .limit(20)
)
