from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3241000, "high").when(F.col("budget") >= 1885000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .orderBy("dept_name", F.col("budget_range").desc(), "budget_bucket")
    .limit(3)
)
