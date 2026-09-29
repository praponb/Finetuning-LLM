from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
    .orderBy(F.col("l3_count").desc(), "job_title")
)
