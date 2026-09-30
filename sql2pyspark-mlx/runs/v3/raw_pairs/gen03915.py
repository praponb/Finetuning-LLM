from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.location").isin(["London", "Bangkok", "New York"]))
    .groupBy(
        F.when(F.col("d.budget") >= 3316000, "high").when(F.col("d.budget") >= 1093000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.dept_name"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
    .orderBy("budget_bucket")
)
