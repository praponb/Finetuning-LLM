from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Engineering", "HR"]))
    .groupBy(
        F.when(F.col("budget") >= 4072000, "high").when(F.col("budget") >= 1151000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("num_departments"))
    .filter(F.col("num_departments") > 8)
    .select("budget_bucket", "num_departments")
)
