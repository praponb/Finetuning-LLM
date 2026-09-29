from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Bangkok"]))
    .groupBy(
        F.when(F.col("budget") >= 3362000, "high").when(F.col("budget") >= 951000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
        "location",
    )
    .agg(F.sum("budget").alias("total_budget"))
)
