from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "London") & F.col("dept_name").isNotNull())
    .groupBy(
        F.when(F.col("budget") >= 3452000, "high").when(F.col("budget") >= 1254000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
    .orderBy(F.col("unique_dept_names").desc())
)
