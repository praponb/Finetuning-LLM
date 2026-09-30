from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .groupBy("manager_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.min("hire_date").alias("first_hire_date"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") >= 82000)
)
