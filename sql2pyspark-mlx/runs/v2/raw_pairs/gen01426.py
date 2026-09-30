from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("is_remote")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("stddev_salary") < 42)
    .select("is_remote", "stddev_salary", "max_salary")
    .orderBy("stddev_salary", "max_salary")
)
