from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "HR", "Finance"]))
    .withColumn("location_title", F.initcap(F.trim("location")))
)

summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3957000, "high").when(F.col("budget") >= 1342000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sort_array(F.collect_set("location")).alias("location_list"))
)

result = (
    summary
    .select("budget_bucket", "location_list")
    .orderBy(F.col("budget_bucket").desc())
)
