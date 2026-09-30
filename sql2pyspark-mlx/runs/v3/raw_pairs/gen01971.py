from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.0) & F.col("supplier_name").like("Li%") & (F.lower(F.trim("supplier_name")) == "eurogoods gmbh"))
    .groupBy("city", "country")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        F.avg("rating").alias("avg_rating"),
    )
)
