from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "New York") & (F.col("country") == "UK") & F.col("rating").between(1.9, 4.0))
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
)
