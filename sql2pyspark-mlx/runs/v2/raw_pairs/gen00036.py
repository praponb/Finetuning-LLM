from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(F.col("oi.unit_price").between(220, 290))
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .filter(F.col("quantity_count") >= 2)
    .orderBy("category")
)
