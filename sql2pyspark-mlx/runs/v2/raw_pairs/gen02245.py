from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L1"]) & (F.col("job_title").isNull() | F.col("level").isin(["L2", "L4"])))
    .select("hire_date", "manager_id", "email")
)
