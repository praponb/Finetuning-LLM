from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "quantity",
        "store_id",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("quantity_filled").desc(), F.col("store_id").desc())
)
