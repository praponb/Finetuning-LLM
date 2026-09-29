from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2892000)
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3309000, "high").when(F.col("budget") >= 798000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .orderBy(F.col("dept_name_values").desc())
)
