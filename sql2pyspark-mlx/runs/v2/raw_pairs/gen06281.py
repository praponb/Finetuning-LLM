from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_group", F.when(F.col("location").isin(["Singapore", "Tokyo"]), "Core").otherwise("Other"))
)

summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3521000, "high").when(F.col("budget") >= 687000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)

result = (
    summary
    .select("budget_bucket", "dept_name_list")
    .orderBy("budget_bucket")
    .limit(5)
)
