from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("job_title", ["Analyst", "Intern", "Engineer"])
    .agg(F.avg("salary"))
)
