from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", "job_title")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
