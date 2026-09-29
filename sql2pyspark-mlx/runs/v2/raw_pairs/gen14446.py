from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([2, 3, 4, 6]) | (F.col("p.cost") > 550))
    .select(
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.product_id"),
    )
)
