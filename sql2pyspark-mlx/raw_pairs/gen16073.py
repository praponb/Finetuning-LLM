from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.sum("rating").alias("total_rating"),
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .orderBy("chiang_mai_count")
)
