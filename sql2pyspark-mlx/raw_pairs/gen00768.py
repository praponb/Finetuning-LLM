from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4288000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
    .orderBy(F.col("distinct_dept_names").desc())
)
