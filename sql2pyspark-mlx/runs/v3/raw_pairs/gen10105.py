from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.min("hire_date").alias("first_hire_date"))
    .orderBy(F.col("first_hire_date").desc())
)
