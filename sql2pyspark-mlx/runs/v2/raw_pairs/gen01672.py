from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy("order_id", "total_quantity")
)
