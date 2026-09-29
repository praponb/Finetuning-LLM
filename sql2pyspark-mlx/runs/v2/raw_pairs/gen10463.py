from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .groupBy("is_remote")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 10)
    .select("is_remote", "job_title_list", "cnt")
)
