from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "HR") & ~F.col("location").isin(["Bangkok", "Singapore"]))
    .groupBy("dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("min_budget") >= 2257000)
)
