from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "outer")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .groupBy(
        F.when(F.col("products.price") >= 1000, "high").when(F.col("products.price") >= 210, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.round(F.avg(F.col("order_items.quantity")), 2).alias("avg_quantity"))
)
