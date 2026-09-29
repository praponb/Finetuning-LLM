from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.col("opened_date").alias("event_date"))
    .intersect(df_employees.select(F.col("hire_date").alias("event_date")))
)
