from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        "quantity",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
    .orderBy(F.col("quantity").desc())
)
