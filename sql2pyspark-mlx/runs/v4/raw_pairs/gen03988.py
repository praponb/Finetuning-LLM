from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNull() & F.col("location").isin(["New York", "Singapore", "Berlin"]) & F.col("location").isin(["New York", "Bangkok", "Tokyo"]))
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
    )
    .orderBy("min_budget")
)
