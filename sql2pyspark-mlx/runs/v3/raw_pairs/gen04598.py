from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.salary") < 144000)
    .groupBy(
        F.when(F.col("d.budget") >= 3413000, "high").when(F.col("d.budget") >= 655000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min(F.col("e.salary")).alias("min_salary"),
        F.sum(F.when(F.col("e.job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
    )
    .filter(F.col("designer_count") > 63)
    .orderBy(F.col("min_salary").desc())
)
