from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("product_id").isin([3, 6, 8, 10]))

agg = (
    valid_inventory
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    agg
    .filter(F.col("max_quantity") >= 110)
    .select("product_id", "max_quantity", "first_last_restock")
)
