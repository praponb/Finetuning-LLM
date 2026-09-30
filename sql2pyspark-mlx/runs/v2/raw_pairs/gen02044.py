from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 21]))
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .orderBy(F.col("unit_price_range").desc(), F.col("product_id").desc())
)
