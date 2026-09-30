from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(
        F.when(F.col("d.budget") >= 3547000, "high").when(F.col("d.budget") >= 1819000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.location"),
    )
    .agg(F.sum(F.col("e.salary")).alias("total_salary"))
    .filter(F.col("total_salary") > 740000)
    .orderBy("budget_bucket", "location")
    .limit(50)
)
