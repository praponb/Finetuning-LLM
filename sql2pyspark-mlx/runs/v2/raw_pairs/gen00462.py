from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.max("salary").alias("max_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)
