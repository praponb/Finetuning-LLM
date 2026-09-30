from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2024-01-01")
    .groupBy("level")
    .agg(
        F.avg("salary").alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("salary_range") < 65)
)
