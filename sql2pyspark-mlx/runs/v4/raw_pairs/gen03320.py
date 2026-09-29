from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4237000, "high").when(F.col("budget") >= 1794000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("dept_name").isin(["Finance", "Marketing"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sales"),
    )
    .orderBy("location")
)
