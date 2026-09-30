from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"), "is_remote")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .orderBy(F.col("last_hire_date").desc())
)
