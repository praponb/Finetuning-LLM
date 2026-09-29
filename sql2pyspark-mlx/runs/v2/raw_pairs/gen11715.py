from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget").between(1589000, 3031000))
    .groupBy(
        F.when(F.col("d.budget") >= 4444000, "high").when(F.col("d.budget") >= 837000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("e.level") == "L1", 1)).alias("l1_count"))
    .filter(F.col("l1_count") >= 2)
    .orderBy(F.col("l1_count").desc())
)
