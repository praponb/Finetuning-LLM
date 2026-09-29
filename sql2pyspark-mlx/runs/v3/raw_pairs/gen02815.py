from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-05-01", "2025-03-01"))
    .groupBy("is_remote", "dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .orderBy(F.col("dept_id").desc(), "is_remote")
    .limit(5)
)
