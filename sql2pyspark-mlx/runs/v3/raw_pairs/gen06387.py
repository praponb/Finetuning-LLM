from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.month(F.col("p.launch_date")).isin([5, 10, 12]))
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("p.stock_qty"))
    .orderBy(F.col("stock_qty").desc(), F.col("launch_date").desc())
)
