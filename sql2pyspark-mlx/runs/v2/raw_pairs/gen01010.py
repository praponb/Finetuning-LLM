from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", "job_title")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .filter(F.col("max_salary") < 148000)
    .orderBy("max_salary", "job_title")
)
