from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "is_remote")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .filter(F.col("stddev_salary") < 42)
    .select("level", "is_remote", "stddev_salary")
)
