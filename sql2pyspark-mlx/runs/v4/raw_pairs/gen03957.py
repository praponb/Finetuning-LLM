from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([1, 3, 4]))
    .groupBy("job_title")
    .pivot("level", ["L1", "L5", "L2", "L4"])
    .agg(F.sum("salary"))
)
