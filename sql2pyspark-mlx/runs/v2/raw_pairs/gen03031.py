from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location").isin(["London", "Singapore", "Bangkok"]))
    .groupBy(
        F.when(F.col("d.budget") >= 3839000, "high").when(F.col("d.budget") >= 1551000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.dept_name"),
    )
    .agg(
        F.max(F.col("e.hire_date")).alias("last_hire_date"),
        F.count(F.when(F.col("e.level") == "L1", 1)).alias("l1_count"),
    )
    .orderBy("dept_name")
)
