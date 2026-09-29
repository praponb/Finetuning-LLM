from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .filter(F.col("order_items.quantity") == 4)
    .groupBy(F.col("products.supplier_id"))
    .agg(
        F.count("*").alias("num_order_items"),
        F.round(F.avg(F.col("order_items.unit_price")), 2).alias("avg_unit_price"),
    )
    .orderBy("num_order_items", "supplier_id")
    .limit(10)
)
