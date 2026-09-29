from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([5, 21]))
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)
