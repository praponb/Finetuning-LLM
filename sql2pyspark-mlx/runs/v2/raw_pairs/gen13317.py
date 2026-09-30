from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter((F.col("rating") >= 2.1) & ~F.col("city").isin(["London", "New York", "Berlin"]))
)

city_summary = (
    base
    .groupBy("city")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = (
    city_summary
    .select("city", "avg_rating")
    .orderBy("avg_rating")
)
