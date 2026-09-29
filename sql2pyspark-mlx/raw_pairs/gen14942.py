from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("budget_tier", F.when(F.col("budget") >= 4483000, "HIGH").otherwise("LOW"))
    .rollup("budget_tier")
    .agg(
        F.max("budget").alias("max_budget"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
