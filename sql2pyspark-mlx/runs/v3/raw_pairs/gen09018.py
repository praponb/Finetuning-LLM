from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("location") == "New York") & (F.col("budget") <= 3686000) & (F.col("dept_name") == "Engineering"))
)

summary = (
    base
    .groupBy(
        F.when(F.col("dept_name").isin(["Marketing", "Engineering"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)

result = (
    summary
    .select("dept_name_group", "engineering_count", "stddev_budget")
    .orderBy(F.col("engineering_count").desc(), "dept_name_group")
    .limit(1)
)
