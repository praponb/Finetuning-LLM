from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 167)
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        F.date_trunc("month", "last_restock").alias("last_restock_period"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy("last_restock_period")
)
