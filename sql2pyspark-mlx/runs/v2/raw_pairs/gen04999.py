from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)
