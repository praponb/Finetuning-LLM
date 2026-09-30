from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Singapore"]) & (F.col("location") == "Tokyo"))
    .groupBy("dept_name")
    .agg(F.sort_array(F.collect_set("location")).alias("location_list"))
)
