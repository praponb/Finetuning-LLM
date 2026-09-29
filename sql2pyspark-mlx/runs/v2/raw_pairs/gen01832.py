from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 7, 10]))
    .withColumn("last_restock_label", F.date_format("last_restock", "MMM yyyy"))
)

agg = (
    recent_inventory
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    agg
    .orderBy(F.col("last_restock_month").desc(), "product_id")
    .limit(10)
)
