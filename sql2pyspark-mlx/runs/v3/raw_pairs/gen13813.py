from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 140) & F.col("product_id").isNotNull())
    .groupBy("product_id", "order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("num_order_items") >= 5)
    .drop("num_order_items")
    .orderBy("total_unit_price", "product_id", "order_id")
    .limit(10)
)
