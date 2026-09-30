from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 2, 5]) & (F.col("last_restock") < "2024-03-01")))
    .select(
        "id",
        "last_restock",
        "quantity",
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
    .orderBy("quantity")
)
