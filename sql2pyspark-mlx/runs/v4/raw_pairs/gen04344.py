from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.min("salary").alias("min_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
    .filter(F.col("min_salary") >= 128000)
    .orderBy("first_hire_date", F.col("min_salary").desc())
)
