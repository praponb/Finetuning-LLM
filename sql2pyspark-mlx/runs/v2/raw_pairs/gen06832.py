from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("job_title", ["Engineer", "Designer", "Analyst"])
    .agg(F.max("salary"))
)
