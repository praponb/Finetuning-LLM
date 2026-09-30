from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)
