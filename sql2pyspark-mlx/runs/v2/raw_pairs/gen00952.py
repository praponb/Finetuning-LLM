from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([2, 6]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.count("*").alias("total_count"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 160)
    .orderBy(F.col("total_count").desc(), "max_quantity", "brand")
    .limit(3)
)
