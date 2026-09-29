from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L1", "L3", "L5"])
    .agg(F.avg("salary"))
    .orderBy("job_title")
)
