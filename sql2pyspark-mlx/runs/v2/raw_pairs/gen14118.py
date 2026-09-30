from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("*").alias("num_order_items"),
    )
    .orderBy("max_unit_price", "unit_price_bucket")
)
