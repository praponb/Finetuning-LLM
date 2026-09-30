from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("is_remote") & (F.col("full_name").isNull() | F.col("dept_id").isin([2, 5, 26])))
)

employees_stats = (
    valid_employees
    .groupBy("is_remote")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = employees_stats.select("is_remote", "job_title_list")
