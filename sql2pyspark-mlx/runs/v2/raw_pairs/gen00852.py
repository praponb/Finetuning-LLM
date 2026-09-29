from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & F.col("dept_name").isin(["Sales", "Engineering", "HR"]))
    .groupBy("location")
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
    .orderBy("location")
)
