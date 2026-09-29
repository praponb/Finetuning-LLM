from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Bangkok")
    .groupBy(
        F.when(F.col("budget") >= 4367000, "high").when(F.col("budget") >= 1779000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
    .orderBy(F.col("unique_dept_names").desc(), "budget_bucket")
    .limit(50)
)
