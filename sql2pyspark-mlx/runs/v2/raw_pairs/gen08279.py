from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("order_id") == 23)

agg = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = agg.select("quantity_bucket", "min_quantity")
