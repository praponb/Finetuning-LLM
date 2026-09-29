from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Engineering", "Finance"]) & (F.col("budget") < 3932000))
)

budget_bucket_summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3217000, "high").when(F.col("budget") >= 1625000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.min("budget").alias("min_budget"),
    )
)

result = (
    budget_bucket_summary
    .select("budget_bucket", "location_list", "min_budget")
    .orderBy("budget_bucket")
    .limit(5)
)
