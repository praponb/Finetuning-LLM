from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3555000, "high").when(F.col("budget") >= 1399000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
)
