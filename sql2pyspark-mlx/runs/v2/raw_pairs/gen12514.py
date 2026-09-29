from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["New York", "London"]))
    .groupBy("dept_name")
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
    .orderBy(F.col("dept_name").desc())
)
