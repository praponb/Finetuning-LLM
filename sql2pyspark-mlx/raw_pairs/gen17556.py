from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1340000, 2774000))
    .groupBy(
        F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("total_budget") >= 15301000)
)
