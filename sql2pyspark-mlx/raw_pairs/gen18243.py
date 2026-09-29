from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .pivot("level", ["L1", "L4", "L5"])
    .agg(F.max("salary"))
)
