from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.0) & ~F.col("supplier_name").like("Sia%") & (F.length("supplier_name") >= 13))
    .groupBy("city", "country")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") > 8)
    .orderBy("total_rating", "city")
)
