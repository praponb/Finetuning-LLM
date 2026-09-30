from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("is_remote")
    .pivot("level", ["L1", "L5"])
    .agg(F.avg("salary"))
    .orderBy("is_remote")
)
