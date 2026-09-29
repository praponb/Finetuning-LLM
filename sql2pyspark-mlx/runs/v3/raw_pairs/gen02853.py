from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 145000) & (F.col("dept_id").isin([7, 8, 10, 36]) | F.col("dept_id").isin([12, 24, 29, 36])))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
    )
)
