from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1725000)
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") > 10853000)
    .select("location", "dept_name_values", "total_budget")
    .orderBy("dept_name_values", "location")
    .limit(1)
)
