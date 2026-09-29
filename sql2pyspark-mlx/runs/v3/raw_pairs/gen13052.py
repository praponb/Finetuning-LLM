from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.min("salary").alias("min_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
