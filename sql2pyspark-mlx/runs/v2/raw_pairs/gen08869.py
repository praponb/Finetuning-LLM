from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3643000, 3829000) & F.col("dept_name").isin(["Engineering", "HR", "Finance"]))
    .agg(
        F.max("budget").alias("max_budget"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .orderBy(F.col("dept_name_values").desc(), F.col("pct_bangkok").desc())
)
