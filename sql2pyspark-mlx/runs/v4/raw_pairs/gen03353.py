from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3215000, 3454000))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3119000, "high").when(F.col("budget") >= 1108000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.sum("budget").alias("total_budget"))
)
