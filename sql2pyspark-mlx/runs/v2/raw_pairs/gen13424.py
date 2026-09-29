from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1583000) & F.col("dept_name").isin(["Engineering", "Finance"]) & (F.col("location") == "Chiang Mai"))
    .groupBy("dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
    )
    .filter(F.col("legal_count") < 64)
    .orderBy(F.col("dept_name").desc())
)
