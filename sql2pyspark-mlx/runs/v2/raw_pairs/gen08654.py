from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.min("hire_date").alias("min_hire_date"),
    )
    .orderBy("min_hire_date")
)
