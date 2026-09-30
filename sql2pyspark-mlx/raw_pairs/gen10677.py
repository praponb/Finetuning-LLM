from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)
