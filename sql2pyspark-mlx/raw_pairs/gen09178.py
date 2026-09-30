from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)
