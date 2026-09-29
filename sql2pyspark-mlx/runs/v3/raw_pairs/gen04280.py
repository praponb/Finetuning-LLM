from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget") >= 3732000)
    .groupBy(
        F.when(F.col("d.budget") >= 4117000, "high").when(F.col("d.budget") >= 2026000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
        F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"),
    )
)
