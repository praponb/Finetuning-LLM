from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-03-01")
    .select(
        "store_id",
        "quantity",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy("quantity_filled")
)
