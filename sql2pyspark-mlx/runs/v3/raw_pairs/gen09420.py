from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull() & F.col("budget").isNotNull())
    .groupBy(
        F.when(F.col("budget") >= 3986000, "high").when(F.col("budget") >= 1175000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy("dept_name_values")
)
