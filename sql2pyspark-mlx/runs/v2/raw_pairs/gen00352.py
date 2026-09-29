from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"))
    .orderBy("avg_quantity")
)
