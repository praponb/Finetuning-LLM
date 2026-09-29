from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(200, 240) | (F.col("unit_price") < 370))
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("cnt"),
        F.min("unit_price").alias("min_unit_price"),
    )
)
