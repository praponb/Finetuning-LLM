from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("dept_id").isin([18, 20, 38, 39]))
    .withColumn("full_name_lower", F.lower("full_name"))
)

employees_stats = (
    valid_employees
    .groupBy("job_title", "is_remote")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = employees_stats
