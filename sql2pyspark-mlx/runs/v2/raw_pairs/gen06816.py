from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
    )
    .orderBy("tokyo_rating")
)
