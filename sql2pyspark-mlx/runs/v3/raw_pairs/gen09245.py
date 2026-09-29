from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3427000, "high").when(F.col("budget") >= 1159000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["Singapore", "London"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("sales_budget") > 49)
)
