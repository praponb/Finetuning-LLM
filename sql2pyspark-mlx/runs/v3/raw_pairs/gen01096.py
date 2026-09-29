from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("budget") > 1491000)

summary = (
    recent_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3750000, "high").when(F.col("budget") >= 917000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
    )
)

result = (
    summary
    .filter(F.col("pct_legal") < 55)
    .select("location", "budget_bucket")
    .orderBy("budget_bucket", "location")
)
