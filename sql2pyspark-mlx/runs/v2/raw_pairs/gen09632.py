from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([12, 23, 38]) & (F.col("quantity").between(4, 6) | F.col("product_id").isin([1, 4, 10]))))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .filter(F.col("avg_unit_price") > 40)
    .orderBy("revenue")
)
