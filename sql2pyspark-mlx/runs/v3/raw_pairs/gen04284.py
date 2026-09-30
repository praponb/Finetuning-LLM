from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & (F.col("location") == "Berlin"))
    .groupBy(
        F.when(F.col("budget") >= 4134000, "high").when(F.col("budget") >= 1505000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") < 17440000)
    .orderBy(F.col("dept_name").desc(), "total_budget")
)
