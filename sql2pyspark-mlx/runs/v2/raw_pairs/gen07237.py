from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .filter(F.col("chiang_mai_count") < 40)
    .select("country", "chiang_mai_count")
)
