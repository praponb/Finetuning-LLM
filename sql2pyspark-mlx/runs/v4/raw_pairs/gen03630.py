from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1121000, 3877000))
    .groupBy(
        F.when(F.col("budget") >= 3817000, "high").when(F.col("budget") >= 1410000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("dept_name").isin(["HR", "Sales"]), "group_a").otherwise("other").alias("dept_name_group"),
        F.when(F.col("location").isin(["London", "Bangkok"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .orderBy(F.col("location_group").desc())
)
