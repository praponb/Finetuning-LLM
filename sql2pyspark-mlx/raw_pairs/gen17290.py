from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        F.month("hire_date").alias("hire_month"),
    )
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .orderBy(F.col("hire_year").desc())
)
