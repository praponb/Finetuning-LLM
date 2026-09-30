from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(
        df_store_sales.alias("s"),
        (F.col("i.store_id") == F.col("s.store_id")) & (F.col("i.product_id") == F.col("s.product_id")),
        "inner",
    )
    .select(F.col("i.store_id"), F.col("i.product_id"), F.col("i.quantity"), F.col("s.units"))
)
