from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .groupingSets(
        [["category", "brand"], ["category"], []],
        F.col("d.category").alias("category"),
        F.col("d.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.unit_price")).alias("avg_unit_price"),
    )
)
