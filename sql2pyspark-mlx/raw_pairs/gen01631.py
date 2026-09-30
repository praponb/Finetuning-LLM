from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("city") == "Singapore")

country_summary = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = (
    country_summary
    .select("country", "rating_count", "avg_rating")
    .orderBy(F.col("avg_rating").desc(), "country")
)
