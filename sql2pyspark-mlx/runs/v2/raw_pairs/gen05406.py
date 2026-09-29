from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pa%"))
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") >= 13)
    .select("city", "rating_count", "total_rating")
    .orderBy("city", "total_rating")
)
