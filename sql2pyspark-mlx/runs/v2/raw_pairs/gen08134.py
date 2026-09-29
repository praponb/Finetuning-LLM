from pyspark.sql import functions as F

src = df_employees.filter(F.col("manager_id").isNull())

agg = (
    src
    .groupBy("job_title", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = (
    agg
    .orderBy("hire_month", "job_title")
    .limit(5)
)
