from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 95) & F.col("store_id").isin([1, 4, 5]) & F.col("quantity").between(182, 201))
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 1)
    .drop("quantity_count")
    .orderBy(F.col("store_id").desc())
)
