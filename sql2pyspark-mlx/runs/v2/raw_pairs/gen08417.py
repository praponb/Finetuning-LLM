from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 9)
    .groupBy("level")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .orderBy(F.col("stddev_salary").desc())
)
