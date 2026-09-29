from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") > 64)
    .orderBy("unit_price_bucket", "order_id")
    .limit(1)
)
