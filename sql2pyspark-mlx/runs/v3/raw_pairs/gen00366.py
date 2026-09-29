from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Manager"]) | F.col("level").isin(["L5", "L2", "L1"]))
)
