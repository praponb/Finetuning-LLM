from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"), "level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .filter(F.col("max_salary") >= 148000)
)
