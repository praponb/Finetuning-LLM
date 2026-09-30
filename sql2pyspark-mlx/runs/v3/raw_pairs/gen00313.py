from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)
