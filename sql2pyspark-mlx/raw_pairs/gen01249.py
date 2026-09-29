from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("source"), "end_date")
    .union(df_employees.select(F.lit("employee").alias("source"), "hire_date"))
    .orderBy("end_date")
)
