from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .pivot("level", ["L4", "L5", "L1"])
    .agg(F.max("salary"))
)
