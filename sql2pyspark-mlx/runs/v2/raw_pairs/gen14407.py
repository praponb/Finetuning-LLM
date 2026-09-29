from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .withColumn("location_title", F.initcap(F.trim("location")))
)

departments_stats = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3614000, "high").when(F.col("budget") >= 1199000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"))
)

result = (
    departments_stats
    .filter(F.col("sales_count") >= 9)
    .select("budget_bucket", "sales_count")
)
