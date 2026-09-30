from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "is_remote", "manager_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
    .filter(F.col("avg_salary") > 53)
)
