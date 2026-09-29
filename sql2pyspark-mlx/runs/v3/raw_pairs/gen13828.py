from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([7, 11, 12]))
    .groupBy("job_title")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)
