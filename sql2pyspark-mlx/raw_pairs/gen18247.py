from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & F.col("hire_date").between("2025-08-01", "2025-09-16"))
    .groupBy("level")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .filter(F.col("stddev_salary") > 28)
    .orderBy(F.col("stddev_salary").desc())
)
