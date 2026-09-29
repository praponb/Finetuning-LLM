from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") > 62000)
    .select("job_title", "level_values", "max_salary")
)
