from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)
