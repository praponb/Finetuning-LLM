from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Analyst", "Director", "Intern"])
    .agg(F.avg("salary"))
    .orderBy("level")
)
