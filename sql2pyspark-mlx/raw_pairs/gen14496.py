from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(F.max("unit_price").alias("max_unit_price"))
    .filter(F.col("max_unit_price") >= 390)
)
