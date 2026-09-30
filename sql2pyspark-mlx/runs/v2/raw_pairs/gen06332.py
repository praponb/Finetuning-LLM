from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Sales", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 3121000, "high").when(F.col("budget") >= 668000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("num_departments"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("unique_locations") >= 9)
)
