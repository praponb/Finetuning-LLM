from pyspark.sql import functions as F

src = df_departments.filter(F.col("dept_name").isin(["Legal", "Sales", "Finance"]))

summary = (
    src
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = (
    summary
    .orderBy("location")
    .limit(50)
)
