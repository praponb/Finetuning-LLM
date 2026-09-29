from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("order_id").isin([10, 30])).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.price"), F.col("p.cost"), F.col("p.launch_date"))
    .orderBy(F.col("cost").desc(), "id")
    .limit(50)
)
