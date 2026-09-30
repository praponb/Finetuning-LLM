from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.month(F.col("p.launch_date")).isin([4, 5, 12]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("sqft").desc(), "id")
    .limit(5)
)
