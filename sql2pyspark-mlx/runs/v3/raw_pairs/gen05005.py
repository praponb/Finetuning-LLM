from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
    .filter(F.col("manager_count") > 4)
)
