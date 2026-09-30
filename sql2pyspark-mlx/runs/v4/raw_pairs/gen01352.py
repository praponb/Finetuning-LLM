from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 3389000, "high").when(F.col("d.budget") >= 725000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum(F.when(F.col("e.level") == "L1", 1).otherwise(0)).alias("l1_count"))
    .orderBy("l1_count")
)
