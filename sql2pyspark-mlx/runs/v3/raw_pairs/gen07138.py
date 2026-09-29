from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
    .orderBy("dept_name")
)
