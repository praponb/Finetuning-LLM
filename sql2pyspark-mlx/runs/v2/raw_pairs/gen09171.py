from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 4490000, "high").when(F.col("d.budget") >= 1403000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.dept_name"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("e.level"))).alias("level_list"),
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
    .orderBy(F.col("dept_name").desc())
)
