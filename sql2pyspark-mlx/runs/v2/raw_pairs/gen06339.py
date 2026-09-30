from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("supplier_name").isNotNull() & F.col("supplier_name").like("%Traders%")) | (F.col("rating") <= 2.6))
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
    .orderBy("stddev_rating")
)
