from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("unit_price_range") > 52)
)
