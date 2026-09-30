from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([2, 4, 10]))
    .select(
        "id",
        "quantity",
        "store_id",
        "last_restock",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
    .orderBy("quantity", "store_id", "id")
    .limit(20)
)
