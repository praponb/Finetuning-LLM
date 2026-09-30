from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([10, 19, 34]))
    .groupBy("job_title")
    .pivot("level", ["L4", "L1", "L2"])
    .agg(F.avg("salary"))
)
