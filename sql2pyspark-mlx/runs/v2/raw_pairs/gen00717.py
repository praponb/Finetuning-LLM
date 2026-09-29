from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 7)
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 450, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") < 34)
)
