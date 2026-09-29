from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .pivot("level", ["L2", "L1", "L5", "L4"])
    .agg(F.max("salary"))
    .orderBy("hire_year")
)
