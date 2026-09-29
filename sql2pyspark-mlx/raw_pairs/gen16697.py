from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(86000, 103000))
    .groupBy("is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") > 58000)
    .drop("min_salary")
)
