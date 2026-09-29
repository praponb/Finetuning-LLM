from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.min("rating").alias("min_rating"),
    )
)
