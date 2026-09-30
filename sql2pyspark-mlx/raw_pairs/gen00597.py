from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.max("salary").alias("max_salary"),
        F.min("salary").alias("min_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .orderBy("job_title")
    .limit(5)
)
