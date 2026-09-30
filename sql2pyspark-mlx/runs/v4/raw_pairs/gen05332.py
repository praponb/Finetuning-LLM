from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Finance"]) & (F.col("dept_name") == "Finance"))
    .agg(
        F.count("budget").alias("budget_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)
