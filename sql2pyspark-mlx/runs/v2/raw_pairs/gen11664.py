from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost") >= 450)
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
    )
    .orderBy("stock_qty")
)
