from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 17)
    .groupBy("level", "is_remote")
    .agg(F.min("hire_date").alias("min_hire_date"))
    .orderBy("level")
)
