from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 140)
    .groupBy("order_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("order_item_count"),
    )
    .filter(F.col("order_item_count") >= 4)
    .orderBy("order_item_count", "total_count")
)
