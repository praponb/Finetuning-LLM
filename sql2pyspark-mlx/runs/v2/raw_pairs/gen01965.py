from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 7.3)
    .orderBy("launch_year")
    .limit(10)
)
