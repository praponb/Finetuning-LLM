from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") > 8) & (F.col("city") != "Singapore"))
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
    )
    .orderBy(F.col("jp_count").desc(), "chiang_mai_rating")
)
