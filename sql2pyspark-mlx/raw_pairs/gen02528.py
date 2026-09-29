from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.col("hire_date").alias("event_date"))
    .unionByName(df_customers.select(F.col("signup_date").alias("event_date")))
    .distinct()
    .orderBy("event_date")
)
