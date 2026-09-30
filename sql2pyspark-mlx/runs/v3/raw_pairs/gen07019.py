from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 360)
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .filter(F.col("total_quantity") < 21)
    .select("order_id", "unit_price_bucket", "total_quantity", "max_unit_price")
)
