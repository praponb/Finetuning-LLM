from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("full_name").isNotNull() & ((F.col("job_title") != "Manager") | F.col("level").isin(["L5", "L1", "L4"])))
)

employees_stats = (
    base
    .groupBy("level")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    employees_stats
    .filter(F.col("stddev_salary") < 79)
    .select("level", "stddev_salary", "level_values")
)
