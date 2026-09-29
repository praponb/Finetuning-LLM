from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("location").isin(["Chiang Mai", "Tokyo"]), "priority").otherwise("other").alias("location_group"),
        "dept_name",
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy(F.col("dept_name").desc(), F.col("location_group").desc())
)
