from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)
