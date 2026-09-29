from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .groupBy("city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
    )
)
