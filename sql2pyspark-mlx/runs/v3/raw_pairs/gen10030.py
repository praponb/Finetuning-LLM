from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .withColumn("location_prefix", F.substring("location", 1, 2))
)

summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3333000, "high").when(F.col("budget") >= 642000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
)

result = (
    summary
    .filter(F.col("min_budget") >= 3491000)
    .select("budget_bucket", "min_budget")
    .orderBy("budget_bucket", "min_budget")
)
