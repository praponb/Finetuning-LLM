from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id") == 4)
    .groupBy(F.col("p.brand"), F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy("brand", F.col("quantity_range").desc(), "launch_month")
    .limit(5)
)
