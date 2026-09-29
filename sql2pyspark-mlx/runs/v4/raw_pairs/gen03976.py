from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.upper(F.col("e.full_name")).like("%ACK%"))
    .groupBy(
        F.when(F.col("d.budget") >= 4133000, "high").when(F.col("d.budget") >= 663000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("d.dept_name").isin(["Marketing", "Engineering"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
)
