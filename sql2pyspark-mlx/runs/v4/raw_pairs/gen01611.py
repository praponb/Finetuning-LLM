from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
    )
)
