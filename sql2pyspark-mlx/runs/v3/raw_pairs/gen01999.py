from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.col("dept_id").isin([1, 4]) & F.col("level").isin(["L1", "L4", "L2"])))
)
