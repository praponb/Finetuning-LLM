from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.last_day(F.col("i.last_restock")).alias("last_restock_month_end"),
    )
    .orderBy(F.col("product_id").desc())
)
