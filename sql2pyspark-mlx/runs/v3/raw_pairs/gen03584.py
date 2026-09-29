from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .pivot("level", ["L2", "L5", "L3", "L1"])
    .agg(F.max("salary"))
    .orderBy("hire_month")
)
