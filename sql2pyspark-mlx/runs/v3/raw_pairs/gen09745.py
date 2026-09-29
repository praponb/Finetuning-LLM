from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3966000, "high").when(F.col("budget") >= 882000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("cnt") > 4)
    .orderBy("dept_name")
)
