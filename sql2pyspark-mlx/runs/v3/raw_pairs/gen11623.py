from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
)
