from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "HR", "Finance"]))
    .groupBy("location")
    .agg(
        F.count("*").alias("department_count"),
        F.count("budget").alias("budget_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)
