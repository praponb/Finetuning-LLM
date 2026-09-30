from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([6, 9]) & F.col("product_id").isin([5, 9]) & (F.year("last_restock") == 2023))
)

agg = (
    recent_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 204, "high").when(F.col("quantity") >= 119, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    agg
    .select("product_id", "quantity_bucket", "first_last_restock", "stddev_quantity")
    .orderBy(F.col("stddev_quantity").desc())
)
