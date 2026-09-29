from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.dept_id").isin([16, 27]))
    .groupBy(
        F.when(F.col("d.budget") >= 3554000, "high").when(F.col("d.budget") >= 1516000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("d.location").isin(["Bangkok", "Chiang Mai"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.min(F.col("e.hire_date")).alias("min_hire_date"),
    )
)
