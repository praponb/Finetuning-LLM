from pyspark.sql import functions as F

recent_departments = df_departments.filter((F.col("dept_name") != "Legal") | (F.col("budget") < 2730000))

agg = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4338000, "high").when(F.col("budget") >= 975000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
)

result = (
    agg
    .orderBy("budget_bucket")
    .limit(10)
)
