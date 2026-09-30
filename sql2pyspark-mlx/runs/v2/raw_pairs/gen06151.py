from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 113000)
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
