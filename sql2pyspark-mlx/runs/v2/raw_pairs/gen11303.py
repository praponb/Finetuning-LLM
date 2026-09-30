from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Singapore") & ~F.col("dept_name").isin(["Marketing", "Finance", "HR"]))
    .agg(
        F.count("budget").alias("budget_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy("budget_count")
)
