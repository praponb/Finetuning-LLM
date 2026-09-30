from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 50, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)
