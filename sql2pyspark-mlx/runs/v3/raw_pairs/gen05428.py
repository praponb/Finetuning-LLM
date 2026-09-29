from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L1", "L2", "L5"])
    .agg(F.max("salary"))
)
