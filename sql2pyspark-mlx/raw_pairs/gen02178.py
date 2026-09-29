from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
)
