from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .pivot("job_title", ["Analyst", "Director", "Manager"])
    .agg(F.avg("salary"))
    .orderBy("hire_quarter")
)
