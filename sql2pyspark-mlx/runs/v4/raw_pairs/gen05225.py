from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 190) & F.col("product_id").isin([16, 20, 29]) & (F.col("order_id") == 37))
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .orderBy(F.col("quantity_bucket").desc(), "product_id")
)
