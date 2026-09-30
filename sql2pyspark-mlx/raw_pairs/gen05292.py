from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 3) & (F.col("product_id").isNotNull() | F.col("order_id").isin([16, 19, 39])))
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("order_item_count"))
    .orderBy("quantity_bucket")
)
