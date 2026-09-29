from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .select(
        "id",
        "quantity",
        "last_restock",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
    )
)
