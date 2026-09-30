from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 79000)
    .groupBy("job_title")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"))
    .orderBy("level_values", "job_title")
)
