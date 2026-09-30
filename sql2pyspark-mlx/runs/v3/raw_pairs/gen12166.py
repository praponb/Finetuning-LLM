from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .groupBy(
        F.when(F.col("budget") >= 3878000, "high").when(F.col("budget") >= 1051000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("cnt", "budget_bucket", "dept_name")
    .limit(5)
)
