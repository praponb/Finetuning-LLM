from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 390) & F.col("order_id").isin([19, 26, 28, 36]))
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.min("unit_price").alias("min_unit_price"))
)
