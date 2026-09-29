from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .pivot("level", ["L5", "L3"])
    .agg(F.sum("salary"))
)
