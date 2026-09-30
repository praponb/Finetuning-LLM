from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") < "2025-07-01") | F.col("manager_id").isNull())
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
    .orderBy("dept_id")
)
