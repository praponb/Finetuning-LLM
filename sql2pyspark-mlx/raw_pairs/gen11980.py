from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3761000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("budget_per_finance"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("budget_per_finance") >= 0)
    .orderBy(F.col("distinct_dept_names").desc())
)
