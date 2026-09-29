from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-09-27").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
)

result = (
    base
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.id", "a.product_id", "a.last_restock", F.col("d.stock_qty"))
)
