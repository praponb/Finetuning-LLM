from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.brand"),
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
)
