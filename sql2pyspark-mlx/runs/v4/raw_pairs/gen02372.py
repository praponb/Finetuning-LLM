from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("delivered_date").alias("event_date"))
    .subtract(df_employees.select(F.col("hire_date").alias("event_date")))
    .orderBy("event_date")
)
