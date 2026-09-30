from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-06-01", "2025-04-06"))
    .groupBy("job_title")
    .pivot("level", ["L4", "L3", "L1", "L5"])
    .agg(F.avg("salary"))
)
