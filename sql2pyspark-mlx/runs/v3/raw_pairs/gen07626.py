from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Engineering", "Legal"]), "core").otherwise("other").alias("dept_name_group"),
        F.when(F.col("budget") >= 3956000, "high").when(F.col("budget") >= 1231000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 2)
    .orderBy("dept_name_group")
)
