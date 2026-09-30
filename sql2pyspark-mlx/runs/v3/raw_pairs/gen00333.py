from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("job_title").isNotNull())

agg = (
    valid_employees
    .groupBy("is_remote")
    .agg(F.sum("salary").alias("total_salary"))
)

result = agg.orderBy(F.col("total_salary").desc())
