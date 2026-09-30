from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.year("hire_date") >= 2025) & (~F.col("is_remote") | ~F.col("is_remote")))
    .groupBy("job_title")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)
