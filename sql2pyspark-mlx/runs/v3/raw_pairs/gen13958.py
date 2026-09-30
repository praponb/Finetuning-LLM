from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum("salary").alias("total_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("total_salary") < 568000)
)
