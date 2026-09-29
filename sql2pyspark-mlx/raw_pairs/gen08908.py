from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isNull()), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("budget") >= 3775000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "dept_name",
        "location",
    )
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_rows"),
        F.count("*").alias("event_count"),
    )
)
