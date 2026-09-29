from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment")
    .distinct()
    .crossJoin(df_employees.select("job_title").distinct())
    .orderBy("segment", "job_title")
)
