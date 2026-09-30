from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3281000)
    .rollup(
        F.when(F.col("budget") >= 4059000, "high").when(F.col("budget") >= 1822000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["Chiang Mai", "Singapore"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
        F.avg("budget").alias("avg_budget"),
    )
    .orderBy("unique_dept_names")
)
