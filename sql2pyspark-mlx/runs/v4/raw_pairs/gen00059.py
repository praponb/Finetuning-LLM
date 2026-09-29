from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(F.max("unit_price").alias("max_unit_price"))
    .filter(F.col("max_unit_price") >= 380)
    .orderBy(F.col("order_id").desc(), "product_id", "unit_price_bucket")
    .limit(1)
)
