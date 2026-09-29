from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & F.col("supplier_name").like("%Supply%"))
    .groupBy("country")
    .agg(
        F.max("rating").alias("max_rating"),
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
    .filter(F.col("max_rating") < 4.1)
)
