from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.hire_date") >= "2023-07-21")
    .groupBy(
        F.col("d.dept_name"),
        F.when(F.col("d.budget") >= 3069000, "high").when(F.col("d.budget") >= 1984000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
    .filter(F.col("pct_l3") > 43)
)
