from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok", "Singapore"]))
    .withColumn("is_engineering", F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))
)

result = (
    base
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
    )
    .orderBy(F.col("total_budget").desc_nulls_first())
)
