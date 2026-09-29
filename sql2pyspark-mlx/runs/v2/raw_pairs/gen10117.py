from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
    .orderBy("avg_salary")
)
