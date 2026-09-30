from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .filter(F.col("oi.product_id").isNotNull())
    .join(df_products.filter(F.col("brand") == "Acme").alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("oi.order_id") == 26)
    .select(F.col("oi.id"), F.col("oi.unit_price"), F.col("oi.product_id"))
    .orderBy("product_id", "id")
    .limit(5)
)
