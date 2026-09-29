from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([5, 10, 20]))
    .select("job_title")
    .intersect(df_employees.filter(F.col("manager_id").isNull()).select("job_title"))
    .orderBy("job_title")
)
