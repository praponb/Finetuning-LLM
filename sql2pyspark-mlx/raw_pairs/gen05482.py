from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity") == 194)
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 243, "High").when(F.col("quantity") >= 47, "Medium").otherwise("Low"))
)

agg = (
    base
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    agg
    .select("store_id", "avg_quantity")
    .orderBy(F.col("store_id").desc(), F.col("avg_quantity").desc())
)
