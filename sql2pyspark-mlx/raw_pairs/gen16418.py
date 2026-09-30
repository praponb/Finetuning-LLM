from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(
        F.when(F.col("d.location").isin(["Berlin", "Bangkok"]), "tier_1").otherwise("other").alias("location_group"),
        F.when(F.col("d.budget") >= 3505000, "high").when(F.col("d.budget") >= 1946000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min(F.col("e.salary")).alias("min_salary"))
)
