from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.dept_name").isin(["Engineering", "Legal", "Sales"]))
    .groupBy(
        F.when(F.col("d.budget") >= 3527000, "high").when(F.col("d.budget") >= 861000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.max(F.col("e.salary")).alias("max_salary"))
    .orderBy("max_salary", "budget_bucket")
)
