from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Sup%") & (F.col("city") == "Bangkok"))
    .groupBy("city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy("city")
)
