from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & F.col("budget").between(2441000, 3761000))
)

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 4089000, "high").when(F.col("budget") >= 1889000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["London", "Berlin"]), "group_a").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
)

result = summary.orderBy("location_group", F.col("marketing_count").desc())
