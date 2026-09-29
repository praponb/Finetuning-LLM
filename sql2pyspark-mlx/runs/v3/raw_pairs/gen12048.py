from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("level", ["L5", "L4", "L1"])
    .agg(F.count("salary"))
)
