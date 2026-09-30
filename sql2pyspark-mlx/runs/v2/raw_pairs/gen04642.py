from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Finance"]))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3130000, "high").when(F.col("budget") >= 1441000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
)
