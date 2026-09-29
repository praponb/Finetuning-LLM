from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.min("salary").alias("min_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)
