from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("location").isin(["Berlin", "Chiang Mai"]))
    .groupBy("dept_name", "location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.max("budget").alias("max_budget"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") < 1)
    .drop("tokyo_count")
)
