from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 3) | F.month("hire_date").isin([2, 9, 11]))
    .withColumn("salary_rounded", F.round("salary", 2))
)
