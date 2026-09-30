from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id", "is_remote")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
