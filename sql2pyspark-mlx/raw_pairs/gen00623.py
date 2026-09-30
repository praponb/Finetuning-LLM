from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Chiang Mai") & (F.col("budget") < 3788000))
    .groupBy("dept_name", "location")
    .agg(
        F.max("budget").alias("max_budget"),
        F.count("budget").alias("budget_count"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
    )
    .filter(F.col("berlin_count") > 2)
    .drop("berlin_count")
)
