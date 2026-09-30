from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull() | F.col("email").like("%@gmail.com"))
    .groupBy("job_title")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)
