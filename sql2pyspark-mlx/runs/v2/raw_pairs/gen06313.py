from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.col("job_title") == "Designer")

agg = (
    recent_employees
    .groupBy("manager_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = agg
