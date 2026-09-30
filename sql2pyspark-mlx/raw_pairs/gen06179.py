from pyspark.sql import functions as F

base = df_inventory.filter(F.col("product_id").isin([1, 5, 8, 9]))

agg = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 246, "high").when(F.col("quantity") >= 98, "mid").otherwise("low").alias("quantity_bucket"),
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    agg
    .filter(F.col("unique_products") >= 9)
    .orderBy("last_restock_month", F.col("quantity_bucket").desc())
    .limit(10)
)
