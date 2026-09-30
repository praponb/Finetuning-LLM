from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("kind"), "status")
    .union(df_employees.select(F.lit("employee").alias("kind"), "job_title"))
    .distinct()
    .orderBy("status")
)
