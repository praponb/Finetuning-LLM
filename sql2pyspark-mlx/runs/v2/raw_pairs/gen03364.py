from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("budget").between(2591000, 2647000) & (F.col("location") == "Bangkok"))
)

location_summary = (
    src
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
)

result = (
    location_summary
    .select("location", "chiang_mai_count")
    .orderBy("location")
    .limit(50)
)
