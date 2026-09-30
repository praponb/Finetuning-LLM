from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.col("s.city") == "Tokyo") & (F.col("s.opened_date") > "2023-08-29"))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.when(F.col("s.region") == "North", 1).otherwise(0).alias("is_north"),
    )
    .orderBy("opened_date")
)
