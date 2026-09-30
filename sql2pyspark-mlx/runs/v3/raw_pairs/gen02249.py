from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("budget") > 3479000) & (~F.col("location").isin(["Singapore", "Chiang Mai", "London"]) | F.col("dept_name").isin(["Sales", "Legal", "Marketing"]))))
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
)
