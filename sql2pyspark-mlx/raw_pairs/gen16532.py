from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .groupBy(
        F.when(F.col("location").isin(["Tokyo", "Berlin"]), "core").otherwise("other").alias("location_group"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.max("budget").alias("max_budget"),
    )
    .orderBy(F.col("max_budget").desc())
)
