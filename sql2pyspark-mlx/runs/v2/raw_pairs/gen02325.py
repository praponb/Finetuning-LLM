from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.round("quantity", 0).alias("quantity_rounded_2"),
    )
)
