from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "is_remote")
    .agg(F.min("hire_date").alias("first_hire_date"))
    .orderBy("level")
)
