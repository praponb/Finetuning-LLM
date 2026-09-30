from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L3", "L1", "L5"])
    .agg(F.sum("salary"))
    .orderBy("job_title")
)
