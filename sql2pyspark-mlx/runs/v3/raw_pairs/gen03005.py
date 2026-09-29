from pyspark.sql import functions as F

base = df_employees.filter(F.col("dept_id").isin([8, 18, 21, 35]))

hire_quarter_summary = (
    base
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = hire_quarter_summary.orderBy(F.col("avg_salary").desc())
