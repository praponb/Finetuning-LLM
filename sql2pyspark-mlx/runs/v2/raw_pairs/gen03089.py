from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("budget").between(1631000, 2609000) & (F.col("location") == "London") & ~F.col("location").isin(["Bangkok", "Chiang Mai", "Berlin"]))
    .withColumn("dept_name_upper", F.upper("dept_name"))
)

summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3966000, "high").when(F.col("budget") >= 2046000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget")).otherwise(0)).alias("berlin_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
)

result = summary
