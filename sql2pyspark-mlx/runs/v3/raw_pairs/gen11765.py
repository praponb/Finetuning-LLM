from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([2, 35, 40]))
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.avg("salary").alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy("salary_range")
)
