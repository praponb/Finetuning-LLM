from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & (F.col("budget") <= 1514000))
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("max_budget") > 3732000)
    .orderBy(F.col("location_values").desc())
)
