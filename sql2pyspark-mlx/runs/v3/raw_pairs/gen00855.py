from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.col("hire_date") < "2023-04-27"))
    .groupBy("is_remote")
    .agg(F.min("hire_date").alias("min_hire_date"))
    .orderBy("min_hire_date", "is_remote")
    .limit(20)
)
