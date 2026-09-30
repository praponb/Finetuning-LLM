from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .groupBy("level")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .orderBy(F.col("level_values").desc())
)
