from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("budget") > 1810000) & F.col("dept_name").isNotNull() & F.col("location").isin(["Singapore", "London"]))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" - "), "location"))
)

departments_stats = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3194000, "high").when(F.col("budget") >= 1473000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
)

result = (
    departments_stats
    .select("budget_bucket", "finance_count")
    .orderBy(F.col("finance_count").desc())
)
