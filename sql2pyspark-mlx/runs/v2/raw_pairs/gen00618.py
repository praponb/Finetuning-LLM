from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4130000, "high").when(F.col("budget") >= 1931000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.min("budget").alias("min_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 3)
    .drop("row_count")
)
