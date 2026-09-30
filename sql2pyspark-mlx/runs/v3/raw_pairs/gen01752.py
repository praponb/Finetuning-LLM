from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]) & (F.year("last_restock") == 2023))
    .select(
        "last_restock",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
)
