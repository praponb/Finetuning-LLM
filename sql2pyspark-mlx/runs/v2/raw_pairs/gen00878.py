from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .select(
        "id",
        "quantity",
        "store_id",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
    .orderBy(F.col("store_id").desc())
)
