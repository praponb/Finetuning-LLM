from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") < 10)
    .groupBy("level")
    .pivot("job_title", ["Director", "Intern", "Analyst"])
    .agg(F.avg("salary"))
)
