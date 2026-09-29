from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(71, 193))
    .select(
        "last_restock",
        "quantity",
        F.round("quantity", 2).alias("quantity_rounded"),
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)
