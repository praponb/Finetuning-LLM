from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L3", "L4", "L2"])
    .agg(F.count("salary"))
    .orderBy("job_title")
)
