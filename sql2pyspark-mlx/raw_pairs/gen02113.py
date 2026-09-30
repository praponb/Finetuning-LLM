from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 3128000, "high").when(F.col("d.budget") >= 689000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("e.level"))).alias("level_list"),
        F.sum(F.col("e.salary")).alias("total_salary"),
    )
    .orderBy("total_salary")
)
