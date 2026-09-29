from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3973000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
