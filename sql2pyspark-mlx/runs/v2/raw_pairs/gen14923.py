from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2024-05-01")
    .groupBy("is_remote")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
