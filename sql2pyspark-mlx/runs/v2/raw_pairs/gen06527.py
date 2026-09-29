from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round("quantity", 0).alias("quantity_rounded"),
    )
    .orderBy("quantity_filled", "id")
    .limit(5)
)
