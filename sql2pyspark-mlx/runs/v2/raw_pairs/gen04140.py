from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("rating") > 3.8)

summary = (
    valid_suppliers
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = (
    summary
    .filter(F.col("rating_count") > 7)
    .select("country", "rating_count", "avg_rating")
)
