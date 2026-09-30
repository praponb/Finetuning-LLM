from pyspark.sql import functions as F

src = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & F.col("location").isin(["Singapore", "London"]))
)

departments_stats = (
    src
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3594000, "high").when(F.col("budget") >= 1364000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = (
    departments_stats
    .select("location", "budget_bucket", "location_values")
    .orderBy("budget_bucket", "location")
    .limit(10)
)
