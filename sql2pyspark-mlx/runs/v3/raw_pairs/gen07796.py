from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]) & F.col("store_id").isin([1, 2, 3, 4]))
)

agg = (
    recent_inventory
    .groupBy("store_id", "product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    agg
    .filter(F.col("total_quantity") > 970)
    .select("store_id", "product_id", "total_quantity")
    .orderBy(F.col("total_quantity").desc(), "store_id", "product_id")
    .limit(5)
)
