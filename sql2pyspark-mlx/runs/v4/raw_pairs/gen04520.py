from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 4)
    .orderBy(F.col("total_count").desc(), F.col("launch_year").desc())
)
