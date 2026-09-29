from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .groupBy(
        F.when(F.col("brand").isin(["Globex", "Umbrella"]), "priority").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .filter(F.col("stock_qty_count") > 4)
    .orderBy("brand_group", F.col("avg_stock_qty").desc())
)
