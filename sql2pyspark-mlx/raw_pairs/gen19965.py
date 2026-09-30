from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") | (F.col("budget") < 2836000))
    .groupBy(
        F.when(F.col("budget") >= 4038000, "high").when(F.col("budget") >= 810000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
        "dept_name",
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
    )
    .orderBy("location", "budget_bucket", "dept_name")
    .limit(20)
)
