from pyspark.sql import functions as F

src = df_employees.filter(F.col("manager_id").isNull() & (F.col("salary") > 118000))

summary = (
    src
    .groupBy("is_remote", F.year("hire_date").alias("hire_year"))
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = (
    summary
    .filter(F.col("stddev_salary") > 36)
    .orderBy(F.col("stddev_salary").desc(), F.col("hire_year").desc())
)
