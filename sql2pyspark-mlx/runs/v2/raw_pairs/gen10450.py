from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", "dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .filter(F.col("l4_count") < 55)
)
