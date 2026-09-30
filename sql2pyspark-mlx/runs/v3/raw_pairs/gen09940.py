from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 84000)
    .groupBy("job_title")
    .pivot("level", ["L2", "L1"])
    .agg(F.max("salary"))
)
