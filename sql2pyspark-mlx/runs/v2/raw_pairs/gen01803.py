from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("level", ["L1", "L3"])
    .agg(F.max("salary"))
)
