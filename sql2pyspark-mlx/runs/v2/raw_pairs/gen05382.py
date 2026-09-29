from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("salary").between(109000, 112000))

summary = (
    valid_employees
    .groupBy("is_remote")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = summary.orderBy("avg_salary")
