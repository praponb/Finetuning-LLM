from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("salary").alias("unique_job_titles"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .orderBy(F.col("unique_job_titles").desc())
)
