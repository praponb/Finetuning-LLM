from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.2) & (F.col("supplier_name").like("%Wholesa%") | (F.col("country") == "US")))
    .groupBy("country", "city")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
)
