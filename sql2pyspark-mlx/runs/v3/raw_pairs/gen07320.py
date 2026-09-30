from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([10, 36]))
    .groupBy("level")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.min("hire_date").alias("min_hire_date"),
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
    )
    .filter(F.col("director_count") < 75)
    .drop("director_count")
)
