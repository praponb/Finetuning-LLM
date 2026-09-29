from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)
